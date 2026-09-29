import os
from flask import Flask, request, jsonify
import logging
from dotenv import load_dotenv
import requests
import base64
import time
import json
from datetime import datetime
# pyrefly: ignore [missing-import]
from sqlalchemy import create_engine
import queue
import threading
import psycopg2 

# --- 1. Configuração Inicial ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

# --- 2. Configuração do Banco de Dados ---
DB_HOST = os.environ.get('DB_HOST')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME')
DB_PORT = os.environ.get('DB_PORT', 25060) 
DB_SCHEMA = os.environ.get('DB_SCHEMA', 'm_db') 

# Prefixa tabelas com o esquema
LOG_TABLE_NAME = f'{DB_SCHEMA}.eventos_bling'
DASH_TABLE_NAME = f'{DB_SCHEMA}.pedidos'
CONTAS_TABLE_NAME = f'{DB_SCHEMA}.bling_contas'
PRODUTOS_TABLE_NAME = f'{DB_SCHEMA}.dim_produtos'
ESTRUTURA_TABLE_NAME = f'{DB_SCHEMA}.dim_estrutura'
FAT_ITENS_VENDA_TABLE = f'{DB_SCHEMA}.fat_itens_venda' 
FILA_TABLE_NAME = f'{DB_SCHEMA}.fila_webhooks' 

db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(
    db_url, 
    pool_size=2,
    max_overflow=1,
    pool_timeout=30, 
    pool_recycle=1800,
    pool_pre_ping=True 
)

# --- 3. SISTEMA DE FILAS E CACHE ---

# Fila infinita em memória
processing_queue = queue.Queue()

# Cache simples em memória
TOKEN_CACHE = {}

# LOCK PARA EVITAR RENOVAÇÃO DUPLA (A CORREÇÃO PRINCIPAL)
TOKEN_LOCK = threading.Lock()

# Novos objetos de controle de fila e desduplicação
QUEUE_LOCK = threading.Lock()
PENDING_TASKS = set()

# Estruturas para Controle de Throttling por conta
THROTTLE_LOCK = threading.Lock()
LAST_REQUEST_TIME = {}
MIN_REQUEST_INTERVAL = 0.4  # segundos (máximo ~2.5 req/s)

def apply_throttling(nome_conta):
    """Garante que haja um intervalo mínimo entre requisições para a mesma conta."""
    sleep_time = 0
    with THROTTLE_LOCK:
        now = time.time()
        last_time = LAST_REQUEST_TIME.get(nome_conta, 0)
        elapsed = now - last_time
        if elapsed < MIN_REQUEST_INTERVAL:
            sleep_time = MIN_REQUEST_INTERVAL - elapsed
            
    if sleep_time > 0:
        time.sleep(sleep_time)
        
    with THROTTLE_LOCK:
        LAST_REQUEST_TIME[nome_conta] = time.time()

def get_db_connection():
    """Obtém conexão do pool."""
    try:
        return engine.raw_connection()
    except Exception as e:
        logging.error(f"Erro CRÍTICO ao pegar conexão do Pool: {e}")
        return None

# --- 4. Gestão de Tokens Otimizada (Com Lock e Cache) ---
def get_bling_token_for_account(nome_conta, force_refresh=False):
    current_ts = time.time()
    
    # 🔴 INICIO DO BLOQUEIO: Só uma thread passa aqui por vez
    with TOKEN_LOCK:
        
        # 1. Tenta pegar do Cache de Memória
        if not force_refresh and nome_conta in TOKEN_CACHE:
            cached = TOKEN_CACHE[nome_conta]
            if cached['expires_at'] > (current_ts + 60): 
                return cached['token']

        # 2. Se não tem no cache, vai no banco
        logging.info(f"[{nome_conta}] Buscando token no banco de dados...")
        conn = get_db_connection()
        if not conn: return None
        
        try:
            with conn.cursor() as cursor:
                sql = f"SELECT client_id, client_secret, access_token, refresh_token, expires_at FROM {CONTAS_TABLE_NAME} WHERE nome_conta = %s"
                cursor.execute(sql, (nome_conta,))
                row = cursor.fetchone()
                
                if not row:
                    logging.error(f"Conta '{nome_conta}' não encontrada.")
                    return None

                column_names = [desc[0] for desc in cursor.description]
                conta_data = dict(zip(column_names, row))
                expires_at = conta_data.get('expires_at') or 0
                
                # Se token válido no banco e não for forçado, atualiza cache e retorna
                if not force_refresh and conta_data['access_token'] and expires_at > (current_ts + 60):
                    TOKEN_CACHE[nome_conta] = {
                        'token': conta_data['access_token'],
                        'expires_at': expires_at
                    }
                    return conta_data['access_token']

                # --- RENOVAÇÃO SEGURA (CORREÇÃO APLICADA AQUI) ---
                logging.info(f"[{nome_conta}] 🔄 Iniciando renovação de token (BLINDADA)...")
                refresh_token = conta_data.get('refresh_token')
                auth_str = f"{conta_data['client_id']}:{conta_data['client_secret']}"
                auth_b64 = base64.b64encode(auth_str.encode()).decode()
                
                url = "https://www.bling.com.br/Api/v3/oauth/token"
                headers = {
                    'Authorization': f'Basic {auth_b64}',
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'enable-jwt': '1'
                }
                payload = {'grant_type': 'refresh_token', 'refresh_token': refresh_token}

                # LOOP DE TENTATIVAS ESPECÍFICO PARA A RENOVAÇÃO
                max_retries_token = 3
                for attempt in range(max_retries_token):
                    apply_throttling(nome_conta)
                    response = requests.post(url, headers=headers, data=payload)
                    
                    if response.status_code == 429:
                        backoff = 2 ** (attempt + 1)
                        logging.warning(f"⏳ [{nome_conta}] 429 Too Many Requests na RENOVAÇÃO DO TOKEN. Dormindo {backoff}s...")
                        time.sleep(backoff)
                        continue # Tenta de novo o POST

                    elif response.status_code == 200:
                        new_data = response.json()
                        new_access_token = new_data['access_token']
                        new_refresh_token = new_data['refresh_token']
                        new_expires_at = int(current_ts + new_data['expires_in'])

                        # Atualiza no banco
                        update_sql = f"UPDATE {CONTAS_TABLE_NAME} SET access_token = %s, refresh_token = %s, expires_at = %s WHERE nome_conta = %s"
                        cursor.execute(update_sql, (new_access_token, new_refresh_token, new_expires_at, nome_conta))
                        conn.commit()
                        
                        # Atualiza Cache
                        TOKEN_CACHE[nome_conta] = {
                            'token': new_access_token,
                            'expires_at': new_expires_at
                        }
                        logging.info(f"[{nome_conta}] ✅ Token renovado com sucesso!")
                        return new_access_token
                    
                    elif response.status_code in [400, 401]:
                        logging.error(f"[{nome_conta}] ❌ ERRO FATAL: Refresh token expirado/inválido. Precisa reautenticar manualmente.")
                        if nome_conta in TOKEN_CACHE: del TOKEN_CACHE[nome_conta]
                        return None
                    
                    else:
                        logging.error(f"[{nome_conta}] Erro API Bling Token: {response.status_code}")
                        return None
                
                logging.error(f"[{nome_conta}] Falha na renovação após {max_retries_token} tentativas.")
                return None

        except Exception as e:
            logging.error(f"Erro Token ({nome_conta}): {e}")
            return None
        finally:
            if conn: conn.close()

# --- 5. API Call (SEM CONEXÃO DE BANCO ABERTA) ---
def get_api_details_v3(endpoint, entity_id, nome_conta):
    # Pega o token (pode usar o banco rapidinho, mas fecha logo em seguida)
    try:
        token = get_bling_token_for_account(nome_conta)
    except Exception as e:
        logging.error(f"Erro ao pegar token: {e}")
        token = None

    if not token:
        # Se falhou o token, lançamos erro para o worker tentar depois ou logar
        raise Exception(f"Falha auth {nome_conta}")

    # Se o token ainda for o antigo opaco (< 100 caracteres), migra automaticamente para JWT
    if len(token) < 100:
        logging.info(f"[{nome_conta}] 🔄 Token legado opaco detectado. Migrando automaticamente para JWT...")
        token = get_bling_token_for_account(nome_conta, force_refresh=True)
        if not token:
            raise Exception(f"Falha ao migrar token para JWT {nome_conta}")

    url = f"https://api.bling.com.br/Api/v3/{endpoint}/{entity_id}"
    headers = {
        'Authorization': f'Bearer {token}',
        'enable-jwt': '1'
    }
    
    tentativa = 1
    max_tentativas = 5 
    
    while tentativa <= max_tentativas:
        try:
            # Aplica throttling proativo
            apply_throttling(nome_conta)
            
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                return response.json().get('data', {})
            
            elif response.status_code == 429:
                backoff_time = 2 ** tentativa
                backoff_time = min(backoff_time, 30)
                logging.warning(f"⏳ [{nome_conta}] Limite API (429) no endpoint {endpoint}. Tentativa {tentativa}. Dormindo {backoff_time}s...")
                time.sleep(backoff_time)
                continue
            
            elif response.status_code >= 500:
                time.sleep(5)
                continue
            elif response.status_code == 404:
                return {}
            elif response.status_code == 401:
                # Se a API nega (401), limpamos o cache para forçar a renovação.
                logging.warning(f"⚠️ API retornou 401 para ID {entity_id}. Limpando cache para renovação...")
                with TOKEN_LOCK: # Protege a limpeza do cache também
                    if nome_conta in TOKEN_CACHE:
                        del TOKEN_CACHE[nome_conta]
                time.sleep(2)
                # Tenta pegar token novo imediatamente
                new_token = get_bling_token_for_account(nome_conta, force_refresh=True)
                if new_token:
                    headers['Authorization'] = f'Bearer {new_token}'
                    continue
                else:
                    raise Exception("Falha ao renovar token após 401")
            else:
                logging.error(f"Erro API Fatal: {response.status_code}")
                raise Exception(f"Erro API: {response.status_code}")
                
        except requests.exceptions.RequestException:
            time.sleep(5)
            continue
        
        tentativa += 1

    raise Exception("Max tentativas API excedido")

# --- Funções SQL (Atualizadas para PostgreSQL ON CONFLICT) ---
def processar_itens_pedido(conn, pedido_id, data_venda, full_data):
    try:
        cursor = conn.cursor()

        # Apaga itens antigos
        cursor.execute(f"DELETE FROM {FAT_ITENS_VENDA_TABLE} WHERE pedido_data_id = %s", (pedido_id,))

        itens = full_data.get('itens', [])
        if not itens:
            return

        # 🔵 AGRUPAR ITENS PELO CODIGO E SOMAR QUANTIDADES
        itens_agrupados = {}

        for item in itens:
            codigo = item.get('codigo', '')
            descricao = item.get('descricao', '')
            quantidade = float(item.get('quantidade', 0))
            valor_unit = float(item.get('valor', 0))

            if codigo not in itens_agrupados:
                itens_agrupados[codigo] = {
                    "codigo": codigo,
                    "descricao": descricao,
                    "quantidade": quantidade,
                    "valor": valor_unit
                }
            else:
                # Soma a quantidade
                itens_agrupados[codigo]["quantidade"] += quantidade

        # 🔵 INSERIR SOMENTE 1 LINHA POR CODIGO
        query_insert = f"""
            INSERT INTO {FAT_ITENS_VENDA_TABLE} 
            (pedido_data_id, codigo, descricao, quantidade, valor_unitario, data_venda)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        for cod, item in itens_agrupados.items():
            cursor.execute(query_insert, (
                pedido_id,
                item["codigo"],
                item["descricao"],
                item["quantidade"],
                item["valor"],
                data_venda
            ))

        logging.info(f"   -> Itens consolidados do pedido {pedido_id} processados.")

    except Exception as e:
        logging.error(f"Erro itens {pedido_id}: {e}")


def atualizar_dashboard(conn, pedido_id, conta_bling, evento, full_data={}, event_date=None):
    val_atualizacao = event_date if event_date else datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if evento == 'order.deleted':
        with conn.cursor() as cursor:
            cursor.execute(f"UPDATE {DASH_TABLE_NAME} SET situacao_id = 9999999, data_atualizacao = %s, ultimo_evento = %s WHERE pedido_data_id = %s", (val_atualizacao, evento, pedido_id))
        return

    # Lógica de Insert/Update (PostgreSQL ON CONFLICT)
    situacao_obj = full_data.get('situacao', {})
    situacao_id = situacao_obj.get('id') if situacao_obj else 0
    if situacao_id is None: situacao_id = 0

    loja_id = full_data.get('loja', {}).get('id') or 0
    numero = full_data.get('numero')
    numero_loja = full_data.get('numeroLoja')
    valor_total = full_data.get('total')

    json_str = json.dumps(full_data, default=str)
    val_criacao = full_data.get('data')

    sql = f"""
        INSERT INTO {DASH_TABLE_NAME} AS d (
            pedido_data_id, conta_bling, loja_id, numero_pedido, numero_loja,
            valor_total, situacao_id, data_criacao, data_atualizacao, ultimo_evento,
            json_completo
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (pedido_data_id) DO UPDATE SET
            conta_bling = EXCLUDED.conta_bling,
            loja_id = EXCLUDED.loja_id,
            numero_pedido = EXCLUDED.numero_pedido,
            numero_loja = EXCLUDED.numero_loja,
            valor_total = EXCLUDED.valor_total,
            situacao_id = EXCLUDED.situacao_id,
            data_atualizacao = EXCLUDED.data_atualizacao,
            data_criacao = COALESCE(d.data_criacao, EXCLUDED.data_criacao),
            ultimo_evento = EXCLUDED.ultimo_evento,
            json_completo = EXCLUDED.json_completo
    """
    values = (pedido_id, conta_bling, loja_id, numero, numero_loja, valor_total, situacao_id, val_criacao, val_atualizacao, evento, json_str)
    
    with conn.cursor() as cursor:
        cursor.execute(sql, values)

    data_venda = val_criacao if val_criacao else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    processar_itens_pedido(conn, pedido_id, data_venda, full_data)

def processar_produto_completo(conn, full_data, nome_conta):
    try:
        cursor = conn.cursor()
        
        id_prod = full_data['id']
        codigo = full_data.get('codigo', '')
        nome = full_data.get('nome', '')
        tipo = full_data.get('tipo', 'P')
        formato = full_data.get('formato', 'S') 
        situacao = full_data.get('situacao', 'A')
        preco_venda = full_data.get('preco', 0)
        
        preco_custo = 0
        if 'fornecedor' in full_data and 'precoCusto' in full_data['fornecedor']:
            preco_custo = full_data['fornecedor']['precoCusto']
            
        estoque_atual = 0
        if 'estoque' in full_data:
            if isinstance(full_data['estoque'], dict):
                estoque_atual = full_data['estoque'].get('saldoVirtualTotal', 0)
            else:
                estoque_atual = full_data['estoque']
            
        json_completo = json.dumps(full_data, default=str)

        logging.info(f"   -> Atualizando Produto {codigo} na conta {nome_conta}...")

        # SQL INSERT/UPDATE (PostgreSQL ON CONFLICT)
        sql_prod = f"""
            INSERT INTO {PRODUTOS_TABLE_NAME} 
            (id_produto, conta_bling, codigo, nome, tipo, formato, situacao, estoque_atual, preco_custo, preco_venda, json_completo, data_atualizacao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id_produto, conta_bling) DO UPDATE SET
                codigo = EXCLUDED.codigo,
                nome = EXCLUDED.nome,
                tipo = EXCLUDED.tipo,
                formato = EXCLUDED.formato,
                situacao = EXCLUDED.situacao,
                estoque_atual = EXCLUDED.estoque_atual,
                preco_custo = EXCLUDED.preco_custo,
                preco_venda = EXCLUDED.preco_venda,
                json_completo = EXCLUDED.json_completo,
                data_atualizacao = NOW()
        """
        cursor.execute(sql_prod, (id_prod, nome_conta, codigo, nome, tipo, formato, situacao, estoque_atual, preco_custo, preco_venda, json_completo))
        
        # Atualiza Estrutura (Kits/Combos) se existir
        if 'estrutura' in full_data and full_data['estrutura'] and 'componentes' in full_data['estrutura']:
            
            # Deleta APENAS a estrutura desta conta para este produto
            cursor.execute(f"DELETE FROM {ESTRUTURA_TABLE_NAME} WHERE id_pai = %s AND conta_bling = %s", (id_prod, nome_conta))
            
            componentes = full_data['estrutura']['componentes']
            for comp in componentes:
                id_filho = comp['produto']['id']
                qtd = comp['quantidade']
                
                cursor.execute(f"INSERT INTO {ESTRUTURA_TABLE_NAME} (id_pai, id_filho, conta_bling, quantidade) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                               (id_prod, id_filho, nome_conta, qtd))
            
            logging.info(f"   -> Estrutura gravada com {len(componentes)} itens.")

    except Exception as e:
        logging.error(f"Erro SQL Produto {full_data.get('id')}: {e}")
        raise e

# --- 6. WORKER: O Consumidor da Fila ---
def worker_processamento():
    """
    Esta função roda em segundo plano.
    Ela processa UM item por vez de forma persistente a partir da tabela
    fila_webhooks no PostgreSQL, garantindo que mesmo se o servidor for
    reiniciado, nenhuma notificação de webhook seja perdida.
    """
    logging.info("🚀 Worker de processamento com Fila Persistente INICIADO.")
    
    # 1. Recupera tarefas que estavam no meio do processamento antes de uma queda/redeploy
    reset_stuck_tasks()
    
    while True:
        task = None
        from_memory = False
        
        # Prioridade 1: Esvazia itens de fallback da memória se houver
        try:
            task = processing_queue.get_nowait()
            from_memory = True
        except queue.Empty:
            pass
            
        # Prioridade 2: Busca próximo item pendente no banco com bloqueio seguro
        if not task:
            task = get_next_task_from_db()
            
        if not task:
            # Fila vazia, dorme 1 segundo para não sobrecarregar CPU/Banco
            time.sleep(1)
            continue
            
        task_id = task.get('id') # None quando veio da memória
        entity_id = task['entity_id']
        conta_bling = task['conta_bling']
        event_type = task['event_type']
        payload_date = task.get('payload_date')
        tentativas = task.get('tentativas', 0)
        
        logging.info(f"⚙️ Processando Fila (ID {task_id or 'RAM'}): {event_type} - {entity_id} ({conta_bling})")
        
        success = False
        error_msg = None
        
        try:
            # 1. Busca API Bling (Stateless / Sem segurar conexão do banco aberta)
            full_data = {}
            if event_type.startswith('order.'):
                full_data = get_api_details_v3('pedidos/vendas', entity_id, conta_bling)
            elif event_type.startswith('product.') or event_type == 'stock.updated':
                full_data = get_api_details_v3('produtos', entity_id, conta_bling)

            # 2. Salva no Banco (Rápido, abre e fecha)
            conn = get_db_connection()
            if conn:
                try:
                    if event_type.startswith('order.'):
                        atualizar_dashboard(conn, entity_id, conta_bling, event_type, full_data, payload_date)
                    elif event_type.startswith('product.') or event_type == 'stock.updated':
                        if full_data: 
                            processar_produto_completo(conn, full_data, conta_bling)
                    
                    # Salva log de evento
                    with conn.cursor() as cursor:
                        cursor.execute(f"""
                            INSERT INTO {LOG_TABLE_NAME} (eventId, data_id, event, conta_bling, data_json, data_created)
                            VALUES (%s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (data_id, eventId) DO UPDATE SET event=EXCLUDED.event
                        """, (f"{event_type}-{entity_id}", entity_id, event_type, conta_bling, json.dumps(task),))
                    
                    conn.commit()
                    success = True
                    logging.info(f"✅ Sucesso Fila: {entity_id}")
                except Exception as e:
                    conn.rollback()
                    error_msg = f"Erro SQL: {e}"
                    logging.error(f"❌ Erro SQL no Worker ({entity_id}): {e}")
                finally:
                    conn.close()
            else:
                error_msg = "Sem conexão DB"
                logging.error("❌ Worker não conseguiu conexão com DB.")
        
        except Exception as e:
            error_msg = f"Erro processamento: {e}"
            logging.error(f"❌ Erro Genérico no Worker ({entity_id}): {e}")
        
        finally:
            if task_id:
                update_task_status(task_id, success, error_msg, tentativas)
            elif not success:
                # Reenfileira na memória se foi fallback e falhou
                processing_queue.put(task)
                
            if from_memory:
                processing_queue.task_done()

def reset_stuck_tasks():
    """Recupera tarefas que ficaram com status 'processando' após um crash ou reinício repentino."""
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE {FILA_TABLE_NAME} SET status = 'pendente', updated_at = NOW() WHERE status = 'processando'")
            conn.commit()
            logging.info("🔄 Verificação de integridade: Tarefas interrompidas recuperadas para 'pendente'.")
        except Exception as e:
            conn.rollback()
            logging.error(f"Erro ao recuperar tarefas no startup: {e}")
        finally:
            conn.close()

def get_next_task_from_db():
    """Busca a próxima tarefa pendente do banco usando FOR UPDATE SKIP LOCKED para segurança concorrente."""
    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT id, conta_bling, event_type, entity_id, payload_date, raw_data, tentativas
                FROM {FILA_TABLE_NAME}
                WHERE status = 'pendente'
                ORDER BY id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """)
            row = cur.fetchone()
            if not row:
                return None
            
            task_id, conta_bling, event_type, entity_id, payload_date, raw_data, tentativas = row
            cur.execute(f"UPDATE {FILA_TABLE_NAME} SET status = 'processando', updated_at = NOW() WHERE id = %s", (task_id,))
            conn.commit()
            return {
                'id': task_id,
                'conta_bling': conta_bling,
                'event_type': event_type,
                'entity_id': entity_id,
                'payload_date': payload_date,
                'raw_data': raw_data,
                'tentativas': tentativas
            }
    except Exception as e:
        conn.rollback()
        logging.error(f"Erro ao buscar tarefa no banco: {e}")
        return None
    finally:
        conn.close()

def update_task_status(task_id, success, error_msg=None, tentativas=0):
    """Atualiza o status final da tarefa na fila persistente."""
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            if success:
                cur.execute(f"""
                    UPDATE {FILA_TABLE_NAME}
                    SET status = 'concluido', processed_at = NOW(), updated_at = NOW()
                    WHERE id = %s
                """, (task_id,))
            else:
                # Se falhou e atingiu 5 tentativas, marca como 'erro', senão volta para 'pendente'
                new_status = 'erro' if tentativas >= 4 else 'pendente'
                cur.execute(f"""
                    UPDATE {FILA_TABLE_NAME}
                    SET status = %s, tentativas = tentativas + 1, ultimo_erro = %s, updated_at = NOW()
                    WHERE id = %s
                """, (new_status, str(error_msg)[:500] if error_msg else None, task_id))
            conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Erro ao atualizar status da tarefa #{task_id}: {e}")
    finally:
        conn.close()

# Inicia a Thread do Worker
threading.Thread(target=worker_processamento, daemon=True).start()

# --- ROTA DE HEALTH CHECK (Com Estatísticas da Fila) ---
@app.route('/health', methods=['GET'])
def health_check():
    pendentes = 0
    processando = 0
    erros = 0
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT status, count(*) 
                    FROM {FILA_TABLE_NAME} 
                    WHERE status IN ('pendente', 'processando', 'erro') 
                    GROUP BY status
                """)
                for row in cur.fetchall():
                    if row[0] == 'pendente': pendentes = row[1]
                    elif row[0] == 'processando': processando = row[1]
                    elif row[0] == 'erro': erros = row[1]
        except:
            pass
        finally:
            conn.close()
            
    return jsonify({
        "status": "healthy",
        "fila_banco": {
            "pendente": pendentes,
            "processando": processando,
            "erro": erros
        },
        "fila_memoria_fallback": processing_queue.qsize()
    }), 200

# --- FUNÇÃO E ROTA DE SINCRONIZAÇÃO HISTÓRICA ---
def sync_orders_for_date_range(conta_bling, data_inicial, data_final):
    # Pega o token
    token = get_bling_token_for_account(conta_bling)
    if not token:
        raise Exception(f"Falha ao obter token para {conta_bling}")

    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json',
        'enable-jwt': '1'
    }
    
    pagina = 1
    total_enfileirados = 0
    
    while True:
        url = "https://api.bling.com.br/Api/v3/pedidos/vendas"
        params = {
            'pagina': pagina,
            'limite': 100,
            'dataInicial': data_inicial,
            'dataFinal': data_final
        }
        
        # Aplica throttling proativo
        apply_throttling(conta_bling)
        
        logging.info(f"[{conta_bling}] Sincronizando página {pagina} de pedidos de vendas...")
        try:
            response = requests.get(url, headers=headers, params=params)
        except Exception as e:
            logging.error(f"Erro na requisição de listagem: {e}")
            break
            
        if response.status_code == 429:
            logging.warning(f"⏳ [{conta_bling}] 429 Too Many Requests na listagem. Dormindo 5s...")
            time.sleep(5)
            continue
            
        if response.status_code != 200:
            logging.error(f"[{conta_bling}] Erro ao listar pedidos na pág {pagina}: {response.status_code}")
            break
            
        res_json = response.json()
        orders = res_json.get('data', [])
        
        if not orders:
            break  # Fim da paginação
            
        conn_sync = get_db_connection()
        if conn_sync:
            try:
                with conn_sync.cursor() as cur_sync:
                    for order in orders:
                        order_id = order.get('id')
                        if not order_id:
                            continue
                        cur_sync.execute(f"""
                            INSERT INTO {FILA_TABLE_NAME} (conta_bling, event_type, entity_id, payload_date, raw_data, status)
                            VALUES (%s, 'order.updated', %s, %s, %s, 'pendente')
                            ON CONFLICT (conta_bling, event_type, entity_id) WHERE status IN ('pendente', 'processando')
                            DO NOTHING
                        """, (conta_bling, order_id, order.get('data'), json.dumps(order)))
                        total_enfileirados += 1
                    conn_sync.commit()
            except Exception as e_sync:
                conn_sync.rollback()
                logging.error(f"Erro ao salvar pedidos sincronizados na fila do banco: {e_sync}")
            finally:
                conn_sync.close()
                    
        pagina += 1
        
        if len(orders) < 100:
            break
            
    return total_enfileirados

@app.route('/sync-orders', methods=['GET', 'POST'])
def sync_orders_route():
    conta = request.args.get('conta')
    data_inicial = request.args.get('data_inicial')
    data_final = request.args.get('data_final')
    
    if not conta or not data_inicial or not data_final:
        return jsonify({
            "status": "error", 
            "message": "Parâmetros 'conta', 'data_inicial' e 'data_final' são obrigatórios. Use YYYY-MM-DD."
        }), 400
        
    def run_sync_in_background(c, di, df):
        try:
            logging.info(f"🔄 [Background Sync] Iniciando sincronização manual de pedidos para {c} de {di} até {df}...")
            total = sync_orders_for_date_range(c, di, df)
            logging.info(f"✅ [Background Sync] Sincronização concluída com sucesso! Total enfileirados: {total}")
        except Exception as err:
            logging.error(f"❌ [Background Sync] Erro na sincronização de {c}: {err}")

    try:
        # Inicia a sincronização em uma thread separada para evitar timeout HTTP do Gunicorn
        threading.Thread(target=run_sync_in_background, args=(conta, data_inicial, data_final), daemon=True).start()
        
        return jsonify({
            "status": "success",
            "message": f"Sincronização de pedidos iniciada em segundo plano para a conta {conta} de {data_inicial} até {data_final}. Acompanhe o processamento nos logs da DigitalOcean."
        }), 200
    except Exception as e:
        logging.error(f"Erro ao iniciar a thread de sincronização: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# --- 7. Handler Leve (Apenas Recebe) ---
@app.route('/webhook-bling', methods=['POST'])
@app.route('/', methods=['POST'])
def handle_bling_webhook():
    conta_bling = request.args.get('conta')
    
    if not conta_bling:
        logging.error("Falta parametro 'conta' na URL do webhook.")
        return jsonify({"status": "error", "message": "Falta parametro 'conta'"}), 400

    try:
        payload = request.get_json()
        if not payload: return jsonify({"message": "Sem JSON"}), 400
    except:
        return jsonify({"message": "JSON Invalido"}), 400

    event_type = payload.get('event')
    data_obj = payload.get('data', {})
    entity_id = data_obj.get('id') or data_obj.get('produto', {}).get('id')
    
    if not entity_id:
        return jsonify({"message": "ID nao encontrado"}), 200

    # --- ENFILEIRAMENTO PERSISTENTE NO BANCO DE DADOS ---
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                insert_sql = f"""
                    INSERT INTO {FILA_TABLE_NAME} (conta_bling, event_type, entity_id, payload_date, raw_data, status)
                    VALUES (%s, %s, %s, %s, %s, 'pendente')
                    ON CONFLICT (conta_bling, event_type, entity_id) WHERE status IN ('pendente', 'processando')
                    DO NOTHING
                    RETURNING id
                """
                cursor.execute(insert_sql, (
                    conta_bling,
                    event_type,
                    entity_id,
                    payload.get('date'),
                    json.dumps(data_obj)
                ))
                row = cursor.fetchone()
                conn.commit()
                if row:
                    logging.info(f"📥 [Banco] Webhook Enfileirado #{row[0]}: {event_type} - {entity_id} ({conta_bling})")
                else:
                    logging.info(f"⏭️ [Banco] Webhook Duplicado Ignorado (Já na fila): {event_type} - {entity_id}")
                return jsonify({"status": "queued"}), 200
        except Exception as e:
            conn.rollback()
            logging.error(f"Erro ao inserir na fila do banco: {e}. Usando fallback na memória.")
        finally:
            conn.close()

    # --- FALLBACK DE EMERGÊNCIA (Caso o banco falhe momentaneamente) ---
    task = {
        'entity_id': entity_id,
        'conta_bling': conta_bling,
        'event_type': event_type,
        'payload_date': payload.get('date'),
        'raw_data': data_obj 
    }
    processing_queue.put(task)
    logging.info(f"📥 [Memória Fallback] Webhook Enfileirado: {event_type} - {entity_id}")
    return jsonify({"status": "queued_fallback"}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)