import streamlit as st
import pandas as pd
import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine
from streamlit_autorefresh import st_autorefresh
from datetime import datetime, timedelta
from PIL import Image
import locale

# ==========================================
# 0. FORÇAR TEMA CLARO E LARANJA (CRÍTICO PARA A TABELA DE BAIXO)
# ==========================================
# Isso cria o arquivo de configuração automaticamente para obrigar
# o Streamlit a desenhar a tabela nativa com fundo branco.
if not os.path.exists(".streamlit"):
    os.makedirs(".streamlit")

with open(".streamlit/config.toml", "w") as f:
    f.write("""
[theme]
base="light"
primaryColor="#FF6700"
backgroundColor="#FFFFFF"
secondaryBackgroundColor="#F8F9FA"
textColor="#31333F"
            
# --- ADICIONADO: FORÇA A DESABILITAÇÃO DE CACHE DE ASSETS PARA PRODUÇÃO ---
[global]
disableAssetCache = true
    """)

# ==========================================
# CONFIGURAÇÃO DA LOGO
LOGO_PATH = 'logo.png' 
# ==========================================

# --- 1. Configuração Inicial ---
st.set_page_config(page_title="Dashboard Pedidos", layout="wide", page_icon="📊")
load_dotenv()
CONFIG_FILE = 'dashboard_config.json'

# --- 2. CSS AVANÇADO (ESTILIZAÇÃO VISUAL) ---
st.markdown("""
    <style>
            
        /* 0. REMOVER ESPAÇO DO TOPO (Padding) */
        .block-container {
            padding-top: 1rem !important;
            padding-bottom: 1rem !important;
            margin-top: 0 !important;
        }
        
        /* 1. Fundo Geral Branco */
        .stApp { background-color: #FFFFFF !important; }
        .stMarkdown, .stText, h1, h2, h3, p, li, span, label, div { color: #31333F !important; }
        
        /* 2. Barra Lateral */
        [data-testid="stSidebar"] { 
            background-color: #F8F9FA !important; 
            border-right: 1px solid #ddd; 
        }

        /* 3. Inputs da Sidebar (Laranja e Branco) */
        .stMultiSelect label, .stNumberInput label, .stDateInput label, .stSelectbox label {
            color: #FF6700 !important;
            font-weight: bold !important;
        }
        .stMultiSelect div[data-baseweb="select"] > div, .stDateInput input, .stNumberInput input {
            background-color: #FFFFFF !important;
            color: #333333 !important;
            border: 1px solid #FF6700 !important;
            border-radius: 5px;
        }
        .stDateInput svg, .stNumberInput svg { fill: #FF6700 !important; }

        /* 4. OTIMIZAÇÃO: Estilização da Tabela de Baixo (Nativa) */
        /* Força borda laranja em volta da tabela */
        [data-testid="stDataFrame"] {
            border: 1px solid #FF6700 !important;
            border-radius: 5px !important;
            padding: 2px !important;
        }
        
        /* Tenta pintar o cabeçalho de laranja (depende da versão do Streamlit, mas o config.toml garante o resto) */
        [data-testid="stDataFrame"] th {
            background-color: #FF6700 !important;
            color: white !important;
        }
        
        /* Esconder elementos nativos do Streamlit */
        footer, .st-emotion-cache-16txtl3, .st-emotion-cache-q8sbsg { display: none !important; }
        #MainMenu, header[data-testid="stHeader"] div:nth-child(3) { display: none !important; }
        button[kind="header"] { display: none !important; }
        .stApp footer { display: none !important; }

        /* 5. Títulos Laranja */
        .orange-highlight { color: #FF6700 !important; }
    </style>
""", unsafe_allow_html=True)

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    return {"refresh_minutes": 5}

def save_config(minutes):
    with open(CONFIG_FILE, 'w') as f:
        json.dump({"refresh_minutes": minutes}, f)

config = load_config()

# --- 3. Conexão com Banco de Dados (CACHE OTIMIZADO) ---
@st.cache_resource
def get_engine():
    # CONFIGURAÇÃO CRÍTICA PARA POSTGRESQL (usando as variáveis .env)
    db_connection_str = f"postgresql+psycopg2://{os.environ.get('DB_USER')}:{os.environ.get('DB_PASSWORD')}@{os.environ.get('DB_HOST')}:{os.environ.get('DB_PORT', 25060)}/{os.environ.get('DB_NAME')}"
    
    # O pool_pre_ping é importante para PostgreSQL
    return create_engine(db_connection_str, pool_recycle=3600, pool_pre_ping=True)

@st.cache_data(ttl=60)
def get_data():
    try:
        engine = get_engine()
        # CORREÇÃO CRÍTICA: Adicionando o prefixo do esquema 'm_db' nas tabelas
        query = """
            SELECT 
                p.pedido_data_id, 
                p.conta_bling, 
                p.numero_pedido, 
                p.valor_total, 
                p.data_criacao,
                COALESCE(ds.nome_situacao, p.situacao_id::text) AS nome_situacao,
                COALESCE(dl.nome_loja, p.loja_id::text) AS nome_loja
            FROM 
                m_db.pedidos AS p
            LEFT JOIN 
                m_db.dim_situacoes AS ds ON p.situacao_id = ds.situacao_id
            LEFT JOIN 
                m_db.dim_lojas AS dl ON p.loja_id = dl.loja_id
            ORDER BY 
                p.data_criacao DESC
        """
        # Usando 'with' garante que a conexão fecha mesmo se der erro
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        if not df.empty:
            df['data_criacao'] = pd.to_datetime(df['data_criacao'])
            df['valor_total'] = pd.to_numeric(df['valor_total'])
            df['situacao_normalizada'] = df['nome_situacao'].astype(str).str.strip()
        return df
    except Exception as e:
        # Se der erro, limpamos o cache para tentar conectar de novo na próxima
        st.cache_data.clear()
        st.error(f"Erro ao conectar no banco: {e}")
        return pd.DataFrame()

# --- 4. Interface ---
st.sidebar.header("⚙️ Configurações")
refresh_minutes = st.sidebar.number_input(
    "Atualizar a cada (minutos):", 
    min_value=1, 
    value=config['refresh_minutes'],
    step=1
)
if refresh_minutes != config['refresh_minutes']:
    save_config(refresh_minutes)
count = st_autorefresh(interval=refresh_minutes * 60 * 1000, key="data_refresh")

st.sidebar.divider()

df = get_data()

if not df.empty:
    st.sidebar.header("🔍 Filtros de Análise")
    
    contas_disponiveis = df['conta_bling'].unique()
    contas_selecionadas = st.sidebar.multiselect(
        "Contas / Lojas", 
        options=contas_disponiveis,
        default=contas_disponiveis
    )

    data_padrao_inicio = df['data_criacao'].min()
    data_padrao_fim = df['data_criacao'].max()
    data_atual = datetime.now()
    
    datas_selecionadas = st.sidebar.date_input(
        "Período de Vendas",
        value=(data_atual, data_atual),
        format="DD/MM/YYYY"
    )
    
    mask_conta = df['conta_bling'].isin(contas_selecionadas)
    df_filtered = df[mask_conta]

    if isinstance(datas_selecionadas, tuple) and len(datas_selecionadas) == 2:
        start_date = pd.to_datetime(datas_selecionadas[0])
        
        # CORREÇÃO AQUI: Adicionamos o final do dia (23:59:59) na data final
        # Assim pegamos pedidos feitos as 13h, 18h, etc.
        end_date = pd.to_datetime(datas_selecionadas[1]) + timedelta(hours=23, minutes=59, seconds=59)

        mask_data = (df_filtered['data_criacao'] >= start_date) & (df_filtered['data_criacao'] <= end_date)
        df_filtered = df_filtered.loc[mask_data]

    # --- 5. Visualização ---
    col_title, col_logo = st.columns([5, 1])
    with col_title:
        st.markdown("<h1 class='orange-highlight'>📊 Dashboard de Pedidos</h1>", unsafe_allow_html=True)
        st.caption(f"Última atualização: {datetime.now().strftime('%H:%M:%S')}")
    with col_logo:
        if os.path.exists(LOGO_PATH):
            try:
                image = Image.open(LOGO_PATH)
                st.image(image, use_container_width=True) 
            except: pass

    st.divider()

    if not df_filtered.empty:
        
        # --- PAINEL DE EXPEDIÇÃO (HTML CUSTOMIZADO) ---
        st.subheader("🚚 Painel de Expedição")

        df_agrupado = df_filtered.groupby(['conta_bling', 'situacao_normalizada']).size().unstack(fill_value=0)
        
        for col in ['Em aberto', 'Em andamento', 'Separado', 'Atendido', 'Finalizado']:
             if col not in df_agrupado.columns:
                 matches = [c for c in df_agrupado.columns if col.lower() in c.lower()]
                 if not matches:
                     df_agrupado[col] = 0

        def soma_col(termo):
            # Soma todas as colunas que contenham o termo (case-insensitive)
            cols = [c for c in df_agrupado.columns if termo.lower() in c.lower()]
            return df_agrupado[cols].sum(axis=1) if cols else pd.Series(0, index=df_agrupado.index)

        resumo = pd.DataFrame({
            'aberto': soma_col('Em aberto'),
            'andamento': soma_col('Em andamento'),
            'separado': soma_col('Separa'), # Captura 'Separação'
            'atendido': soma_col('Atendido') + soma_col('Finalizado') # Soma atendido + finalizado
        })
        resumo['total'] = resumo.sum(axis=1)
        resumo = resumo.sort_values('total', ascending=False)

        # GERAÇÃO DO HTML (SEM INDENTAÇÃO INTERNA PARA EVITAR ERROS)
        html_rows = ""
        for conta, row in resumo.iterrows():
            html_rows += f"<tr><td style='font-weight: bold; color: #333 !important;'>{str(conta).upper()}</td><td><span class='status-val' style='color: #FF4B4B;'>{row['aberto']}</span></td><td><span class='status-val' style='color: #FF8C00;'>{row['andamento']}</span></td><td><span class='status-val' style='color: #0066CC;'>{row['separado']}</span></td><td><span class='status-val' style='color: #00CC96;'>{row['atendido']}</span></td></tr>"

        html_table = f"""
        <style>
        .custom-table {{ width: 100%; border-collapse: collapse; background-color: #FFFFFF !important; border: 1px solid #FF6700; border-radius: 8px; margin-bottom: 20px; }}
        .custom-table th {{ background-color: #FF6700 !important; color: white !important; padding: 15px; text-align: left; font-size: 16px; font-weight: bold; }}
        .custom-table td {{ padding: 18px 15px; border-bottom: 1px solid #eee; font-size: 18px; color: #333 !important; }}
        .custom-table tr:hover {{ background-color: #FFF5EB !important; }}
        .status-val {{ font-weight: bold; font-size: 22px; }}
        </style>
        <table class="custom-table">
        <thead><tr><th style="width: 30%;">CONTA / LOJA</th><th>🔴 EM ABERTO</th><th>🟠 EM ANDAMENTO</th><th>🔵 SEPARADO</th><th>🟢 ATENDIDO</th></tr></thead>
        <tbody>{html_rows}</tbody>
        </table>
        """
        st.markdown(html_table, unsafe_allow_html=True)
        
        st.divider()

        # --- LISTAGEM DETALHADA ---
        st.subheader(f"📦 Listagem Detalhada ({len(df_filtered)})")
        
        tab1, tab2 = st.tabs(["Todos os Pedidos", "Apenas Pendentes"])
        cols = ['data_criacao', 'conta_bling', 'numero_pedido', 'nome_situacao', 'nome_loja', 'valor_total']
        
        col_config = {
            "valor_total": st.column_config.NumberColumn("Valor Total", format="R$ %.2f"),
            "data_criacao": st.column_config.DatetimeColumn("Data Criação", format="DD/MM/YYYY HH:mm"),
            "conta_bling": st.column_config.TextColumn("Conta"),
            "numero_pedido": st.column_config.TextColumn("Pedido"),
            "nome_situacao": st.column_config.TextColumn("Situação"),
            "nome_loja": st.column_config.TextColumn("Loja"),
        }

        with tab1:
            st.dataframe(
                df_filtered[cols].sort_values(by='data_criacao', ascending=False),
                use_container_width=True,
                hide_index=True,
                column_config=col_config
            )
        
        with tab2:
            # Corrigido: Usando a coluna normalizada para o filtro
            mask_pend = df_filtered['situacao_normalizada'].str.contains('Em aberto|Em andamento|Pendente|Separação', case=False, na=False)
            df_pend = df_filtered[mask_pend]
            if not df_pend.empty:
                st.dataframe(
                    df_pend[cols].sort_values(by='data_criacao', ascending=False),
                    use_container_width=True,
                    hide_index=True,
                    column_config=col_config
                )
            else:
                st.info("Nenhum pedido pendente!")

    else:
        st.warning("Sem dados para os filtros selecionados.")