import os
from flask import Flask, request, jsonify
import logging
from dotenv import load_dotenv
import requests
import base64
import time
import json
from datetime import datetime
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

def get_db_connection():
    """Obtém conexão do pool."""
    try:
        return engine.raw_connection()
    except Exception as e:
        logging.error(f"Erro CRÍTICO ao pegar conexão do Pool: {e}")
        return None

# --- 4. Gestão de Tokens Otimizada (Com Lock e Cache) ---
def get_bling_token_for_account(nome_conta):
    current_ts = time.time()
    
    # 🔴 INICIO DO BLOQUEIO: Só uma thread passa aqui por vez
    with TOKEN_LOCK:
        
        # 1. Tenta pegar do Cache de Memória
        if nome_conta in TOKEN_CACHE:
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
                
                # Se token válido no banco, atualiza cache e retorna
                if conta_data['access_token'] and expires_at > (current_ts + 60):
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
                headers = {'Authorization': f'Basic {auth_b64}', 'Content-Type': 'application/x-www-form-urlencoded'}
                payload = {'grant_type': 'refresh_token', 'refresh_token': refresh_token}

                # LOOP DE TENTATIVAS ESPECÍFICO PARA A RENOVAÇÃO
                max_retries_token = 3
                for attempt in range(max_retries_token):
                    response = requests.post(url, headers=headers, data=payload)
                    
                    if response.status_code == 429:
                        logging.warning(f"⏳ [{nome_conta}] 429 Too Many Requests na RENOVAÇÃO DO TOKEN. Dormindo 60s...")
                        time.sleep(60) # Dorme 1 minuto inteiro antes de tentar de novo
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

    url = f"https://api.bling.com.br/Api/v3/{endpoint}/{entity_id}"
    headers = {'Authorization': f'Bearer {token}'}
    
    tentativa = 1
    max_tentativas = 20 
    
    while tentativa <= max_tentativas:
        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                return response.json().get('data', {})
            
            # --- CORREÇÃO AQUI: AUMENTADO TEMPO DE ESPERA PARA 429 ---
            elif response.status_code == 429:
                logging.warning(f"⏳ [{nome_conta}] Limite API (429) no endpoint {endpoint}. Tentativa {tentativa}. Dormindo 60s...")
                time.sleep(60) # Pausa agressiva de 60 segundos
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
                new_token = get_bling_token_for_account(nome_conta)
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
    Ela processa UM item por vez (ou conforme sua lógica), garantindo que
    o banco nunca fique sobrecarregado, mesmo se chegarem 1000 webhooks.
    """
    logging.info("🚀 Worker de processamento INICIADO.")
    
    while True:
        # Pega item da fila (bloqueia se estiver vazia até chegar algo)
        task = processing_queue.get() 
        
        try:
            entity_id = task['entity_id']
            conta_bling = task['conta_bling']
            event_type = task['event_type']
            payload_date = task['payload_date']
            
            logging.info(f"⚙️ Processando Fila: {event_type} - {entity_id}")

            # 1. Busca API (Lento, mas SEM BANCO)
            full_data = {}
            if event_type.startswith('order.'):
                full_data = get_api_details_v3('pedidos/vendas', entity_id, conta_bling)
            elif event_type.startswith('product.'):
                full_data = get_api_details_v3('produtos', entity_id, conta_bling)
            elif event_type == 'stock.updated':
                full_data = get_api_details_v3('produtos', entity_id, conta_bling)

            # 2. Salva no Banco (Rápido, abre e fecha)
            conn = get_db_connection()
            if conn:
                try:
                    if event_type.startswith('order.'):
                        atualizar_dashboard(conn, entity_id, conta_bling, event_type, full_data, payload_date)
                    elif event_type.startswith('product.') or event_type == 'stock.updated':
                        # Apenas salva se os dados foram encontrados (404 é tratado na API)
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
                    logging.info(f"✅ Sucesso Fila: {entity_id}")
                except Exception as e:
                    conn.rollback()
                    logging.error(f"❌ Erro SQL no Worker ({entity_id}): {e}")
                    # Reenfileirar o item se houver erro SQL para nova tentativa
                    processing_queue.put(task) 
                finally:
                    if conn: conn.close() # DEVOLVE A CONEXÃO PRO POOL IMEDIATAMENTE
            else:
                logging.error("❌ Worker não conseguiu conexão com DB. Reenfileirando...")
                processing_queue.put(task)
        
        except Exception as e:
            logging.error(f"❌ Erro Genérico no Worker: {e}")
        
        finally:
            processing_queue.task_done()

# Inicia a Thread do Worker
threading.Thread(target=worker_processamento, daemon=True).start()

# --- ROTA DE HEALTH CHECK (Para a DigitalOcean) ---
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

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

    # --- ENFILEIRAMENTO ---
    task = {
        'entity_id': entity_id,
        'conta_bling': conta_bling,
        'event_type': event_type,
        'payload_date': payload.get('date'),
        'raw_data': data_obj 
    }
    
    processing_queue.put(task)
    
    logging.info(f"📥 Webhook Recebido e Enfileirado: {event_type} - {entity_id} (Fila: {processing_queue.qsize()})")

    return jsonify({"status": "queued"}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)