import os
import pymysql
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

# --- 1. Configuração Inicial ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

# --- 2. Configuração do Banco de Dados ---
DB_HOST = os.environ.get('DB_HOST')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME')

LOG_TABLE_NAME = 'eventos_bling'
DASH_TABLE_NAME = 'pedidos'
CONTAS_TABLE_NAME = 'bling_contas'
PRODUTOS_TABLE_NAME = 'dim_produtos'
ESTRUTURA_TABLE_NAME = 'dim_estrutura'

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"

# Pool ajustado para trabalhar com a Fila
engine = create_engine(
    db_url, 
    pool_size=5,            # Baixo, pois o Worker é sequencial
    max_overflow=10,        # Margem para conexões rápidas de token
    pool_timeout=60, 
    pool_recycle=1800,
    pool_pre_ping=True 
)

# --- 3. ESTRUTURA DE FILA E CACHE ---
# Fila infinita em memória
processing_queue = queue.Queue()

# Cache de Token para não travar o banco buscando token toda hora
TOKEN_CACHE = {}

def get_db_connection():
    try:
        conn = engine.raw_connection()
        original_cursor = conn.cursor
        def dict_cursor_wrapper(*args, **kwargs):
            return original_cursor(pymysql.cursors.DictCursor, *args, **kwargs)
        conn.cursor = dict_cursor_wrapper
        return conn
    except Exception as e:
        logging.error(f"Erro CRÍTICO ao pegar conexão do Pool: {e}")
        return None

# --- 4. Gestão de Tokens (Com Cache de Memória) ---
def get_bling_token_for_account(nome_conta):
    current_ts = time.time()
    
    # 1. Tenta Cache Primeiro (Zero Banco)
    if nome_conta in TOKEN_CACHE:
        cached = TOKEN_CACHE[nome_conta]
        if cached['expires_at'] > (current_ts + 60):
            return cached['token']

    # 2. Se não tem no cache, busca no DB
    conn = get_db_connection()
    if not conn: return None

    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {CONTAS_TABLE_NAME} WHERE nome_conta = %s", (nome_conta,))
            conta_data = cursor.fetchone()

            if not conta_data:
                logging.error(f"Conta '{nome_conta}' não encontrada.")
                return None

            expires_at = conta_data.get('expires_at') or 0
            
            # Valida se o do banco serve
            if conta_data['access_token'] and expires_at > (current_ts + 60):
                TOKEN_CACHE[nome_conta] = {'token': conta_data['access_token'], 'expires_at': expires_at}
                return conta_data['access_token']

            logging.info(f"[{nome_conta}] Renovando token expirado...")
            refresh_token = conta_data.get('refresh_token')
            auth_str = f"{conta_data['client_id']}:{conta_data['client_secret']}"
            auth_b64 = base64.b64encode(auth_str.encode()).decode()
            
            url = "https://www.bling.com.br/Api/v3/oauth/token"
            headers = {'Authorization': f'Basic {auth_b64}', 'Content-Type': 'application/x-www-form-urlencoded'}
            payload = {'grant_type': 'refresh_token', 'refresh_token': refresh_token}

            response = requests.post(url, headers=headers, data=payload)
            if response.status_code != 200:
                logging.error(f"Erro renovação: {response.text}")
                return None
                
            new_data = response.json()
            new_access_token = new_data['access_token']
            new_expires_at = int(current_ts + new_data['expires_in'])

            # Salva no Banco
            update_sql = f"UPDATE {CONTAS_TABLE_NAME} SET access_token = %s, refresh_token = %s, expires_at = %s WHERE nome_conta = %s"
            cursor.execute(update_sql, (new_access_token, new_data['refresh_token'], new_expires_at, nome_conta))
            conn.commit()
            
            # Atualiza Cache
            TOKEN_CACHE[nome_conta] = {'token': new_access_token, 'expires_at': new_expires_at}
            return new_access_token

    except Exception as e:
        logging.error(f"Erro Token ({nome_conta}): {e}")
        return None
    finally:
        conn.close()

# --- 5. API Call (SEM CONEXÃO DE BANCO ABERTA) ---
def get_api_details_v3(endpoint, entity_id, nome_conta):
    # Pega token (Cache ou DB rápido)
    token = get_bling_token_for_account(nome_conta)
    if not token: raise Exception(f"Falha auth {nome_conta}")

    url = f"https://api.bling.com.br/Api/v3/{endpoint}/{entity_id}"
    headers = {'Authorization': f'Bearer {token}'}
    
    tentativa = 1
    max_tentativas = 15 
    
    while tentativa <= max_tentativas:
        try:
            # AQUI O BANCO ESTÁ FECHADO (Seguro para esperar)
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                return response.json().get('data', {})
            elif response.status_code == 429:
                tempo_espera = 3 + (tentativa * 2)
                logging.warning(f"⏳ Limite API (429). Tentativa {tentativa}. Esperando {tempo_espera}s...")
                time.sleep(tempo_espera) 
                continue
            elif response.status_code >= 500:
                logging.warning(f"⚠️ Erro 500 Bling. Tentativa {tentativa}. Sleep 5s...")
                time.sleep(5)
                continue
            elif response.status_code == 404:
                return {} # Retorna vazio, não é erro
            else:
                logging.error(f"Erro API Fatal: {response.status_code}. Tentando novamente em 5s...")
                time.sleep(5)
                # Não dá raise aqui para continuar tentando se for instabilidade temporária
                
        except requests.exceptions.RequestException:
            time.sleep(5)
        
        tentativa += 1

    raise Exception("Max tentativas API excedido")

# --- FUNÇÕES SQL (Executadas APENAS pelo Worker) ---
def processar_itens_pedido(conn, pedido_id, data_venda, full_data):
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM fat_itens_venda WHERE pedido_data_id = %s", (pedido_id,))
        
        if 'itens' in full_data and full_data['itens']:
            query_insert = """
                INSERT INTO fat_itens_venda (pedido_data_id, codigo, descricao, quantidade, valor_unitario, data_venda)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            for item in full_data['itens']:
                cursor.execute(query_insert, (
                    pedido_id, item.get('codigo', ''), item.get('descricao', ''), 
                    item.get('quantidade', 0), item.get('valor', 0), data_venda
                ))
            logging.info(f"   -> Itens {pedido_id} processados.")
    except Exception as e:
        logging.error(f"Erro itens {pedido_id}: {e}")
        # Se der erro aqui, a transação principal vai dar rollback e tentar tudo de novo

def atualizar_dashboard(conn, pedido_id, conta_bling, evento, full_data={}, event_date=None):
    val_atualizacao = event_date if event_date else time.strftime('%Y-%m-%d %H:%M:%S')

    if evento == 'order.deleted':
        with conn.cursor() as cursor:
            cursor.execute(f"UPDATE {DASH_TABLE_NAME} SET situacao_id = 9999999, data_atualizacao = %s, ultimo_evento = %s WHERE pedido_data_id = %s", (val_atualizacao, evento, pedido_id))
        return

    # Lógica de Insert/Update
    situacao_id = full_data.get('situacao', {}).get('id', 0)
    if situacao_id is None: situacao_id = 0
    
    loja_id = full_data.get('loja', {}).get('id') or 0
    json_str = json.dumps(full_data, default=str)
    val_criacao = full_data.get('data')

    sql = f"""
        INSERT INTO {DASH_TABLE_NAME} (
            pedido_data_id, conta_bling, loja_id, numero_pedido, numero_loja,
            valor_total, situacao_id, data_criacao, data_atualizacao, ultimo_evento,
            json_completo
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            conta_bling = VALUES(conta_bling), loja_id = VALUES(loja_id),
            numero_pedido = VALUES(numero_pedido), numero_loja = VALUES(numero_loja),
            valor_total = VALUES(valor_total), situacao_id = VALUES(situacao_id),
            data_atualizacao = VALUES(data_atualizacao),
            data_criacao = COALESCE(data_criacao, VALUES(data_criacao)),
            ultimo_evento = VALUES(ultimo_evento), json_completo = VALUES(json_completo)
    """
    values = (pedido_id, conta_bling, loja_id, full_data.get('numero'), full_data.get('numeroLoja'), 
              full_data.get('total'), situacao_id, val_criacao, val_atualizacao, evento, json_str)
    
    with conn.cursor() as cursor:
        cursor.execute(sql, values)

    data_venda = val_criacao if val_criacao else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    processar_itens_pedido(conn, pedido_id, data_venda, full_data)

def processar_produto_completo(conn, full_data, nome_conta):
    try:
        cursor = conn.cursor()
        id_prod = full_data['id']
        codigo = full_data.get('codigo', '')
        
        estoque_atual = 0
        if 'estoque' in full_data:
            if isinstance(full_data['estoque'], dict):
                estoque_atual = full_data['estoque'].get('saldoVirtualTotal', 0)
            else:
                estoque_atual = full_data['estoque']
        
        preco_custo = 0
        if 'fornecedor' in full_data and 'precoCusto' in full_data['fornecedor']:
            preco_custo = full_data['fornecedor']['precoCusto']

        sql_prod = f"""
            INSERT INTO {PRODUTOS_TABLE_NAME} 
            (id_produto, conta_bling, codigo, nome, tipo, formato, situacao, estoque_atual, preco_custo, preco_venda, json_completo, data_atualizacao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                codigo=VALUES(codigo), nome=VALUES(nome), tipo=VALUES(tipo), formato=VALUES(formato),
                situacao=VALUES(situacao), estoque_atual=VALUES(estoque_atual),
                preco_custo=VALUES(preco_custo), preco_venda=VALUES(preco_venda),
                json_completo=VALUES(json_completo), data_atualizacao=NOW()
        """
        cursor.execute(sql_prod, (
            id_prod, nome_conta, codigo, full_data.get('nome',''), full_data.get('tipo','P'),
            full_data.get('formato','S'), full_data.get('situacao','A'), estoque_atual,
            preco_custo, full_data.get('preco',0), json.dumps(full_data, default=str)
        ))
        
        # Estrutura
        if 'estrutura' in full_data and full_data['estrutura'] and 'componentes' in full_data['estrutura']:
            cursor.execute(f"DELETE FROM {ESTRUTURA_TABLE_NAME} WHERE id_pai = %s AND conta_bling = %s", (id_prod, nome_conta))
            for comp in full_data['estrutura']['componentes']:
                cursor.execute(f"INSERT INTO {ESTRUTURA_TABLE_NAME} (id_pai, id_filho, conta_bling, quantidade) VALUES (%s, %s, %s, %s)", 
                               (id_prod, comp['produto']['id'], nome_conta, comp['quantidade']))
            logging.info(f"   -> Estrutura atualizada.")

    except Exception as e:
        logging.error(f"Erro SQL Produto: {e}")
        raise e

# --- 6. WORKER: O CONSUMIDOR DE FILA (O Salvador do Banco) ---
def worker_processamento():
    """
    Consome a fila sequencialmente.
    - Se a API demorar 20s, o banco fica LIVRE por 20s.
    - Loop Infinito de Retry: Se der erro, dorme e tenta de novo.
    """
    logging.info("🚀 Worker de Webhooks INICIADO.")
    
    while True:
        task = processing_queue.get()
        
        entity_id = task['entity_id']
        conta_bling = task['conta_bling']
        event_type = task['event_type']
        payload_date = task['payload_date']
        
        sucesso = False
        
        # LOOP INFINITO DE PERSISTÊNCIA
        # Só sai daqui quando salvar no banco ou se o pedido não existir (404)
        while not sucesso:
            try:
                logging.info(f"⚙️ Worker Processando: {event_type} - {entity_id}")

                # 1. Busca API (Lento - SEM BANCO)
                full_data = {}
                
                # Otimização: Se for 'order.deleted', não precisamos buscar na API (economiza requisição)
                if event_type == 'order.deleted':
                    full_data = {} 
                else:
                    try:
                        if event_type.startswith('order.'):
                            full_data = get_api_details_v3('pedidos/vendas', entity_id, conta_bling)
                        elif event_type.startswith('product.') or event_type == 'stock.updated':
                            full_data = get_api_details_v3('produtos', entity_id, conta_bling)
                    except Exception as e:
                        logging.error(f"❌ Erro API ({entity_id}): {e}. Tentando novamente em 10s...")
                        time.sleep(10)
                        continue # Volta pro inicio do while not sucesso

                # 2. Salva no Banco (Rápido - ABRE E FECHA)
                conn = get_db_connection()
                if conn:
                    try:
                        if event_type.startswith('order.'):
                            atualizar_dashboard(conn, entity_id, conta_bling, event_type, full_data, payload_date)
                        elif event_type.startswith('product.') or event_type == 'stock.updated':
                            # Se a API retornou vazio por erro 404, não tem como salvar produto
                            if full_data:
                                processar_produto_completo(conn, full_data, conta_bling)
                        
                        conn.commit()
                        logging.info(f"✅ Sucesso Worker: {entity_id}")
                        sucesso = True # UFA! Sai do loop
                    except Exception as e:
                        conn.rollback()
                        logging.error(f"❌ Erro SQL no Worker ({entity_id}): {e}. Tentando DB em 5s...")
                        time.sleep(5)
                    finally:
                        conn.close() 
                else:
                    logging.error("❌ Worker sem conexão DB. Tentando reconectar em 5s...")
                    time.sleep(5)
            
            except Exception as e:
                logging.error(f"❌ Erro Crítico Genérico ({entity_id}): {e}. Tentando em 10s...")
                time.sleep(10)
        
        processing_queue.task_done()

# Inicia o Worker
threading.Thread(target=worker_processamento, daemon=True).start()

# --- 7. ROTA FLASK (Apenas Recebe e Enfileira) ---
@app.route('/webhook-bling', methods=['POST'])
def handle_bling_webhook():
    conta_bling = request.args.get('conta')
    if not conta_bling: return jsonify({"status": "error"}), 400

    try:
        payload = request.get_json()
        if not payload: return jsonify({"message": "Sem JSON"}), 400
    except:
        return jsonify({"message": "JSON Invalido"}), 400

    event_type = payload.get('event')
    data_obj = payload.get('data', {})
    entity_id = data_obj.get('id') or data_obj.get('produto', {}).get('id')
    
    if not entity_id: return jsonify({"message": "ID nao encontrado"}), 200

    # ENFILEIRA PARA O WORKER
    task = {
        'entity_id': entity_id,
        'conta_bling': conta_bling,
        'event_type': event_type,
        'payload_date': payload.get('date')
    }
    
    processing_queue.put(task)
    
    # Resposta instantânea (Bling não fica esperando)
    return jsonify({"status": "queued", "queue_size": processing_queue.qsize()}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)