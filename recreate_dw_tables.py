# recreate_dw_tables.py
import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=adventureworks_dw;"
    "UID=sa;"
    "PWD=123456;"
    "TrustServerCertificate=yes;"
)
cursor = conn.cursor()

# Remover tabelas existentes (na ordem correta por causa das FKs)
print("Removendo tabelas existentes...")
tables = ['fato_vendas', 'dim_produto', 'dim_cliente', 'dim_vendedor',
          'dim_territorio', 'dim_promocao', 'dim_tempo', 'etl_controle']

for table in tables:
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        print(f"  - {table} removida")
    except:
        pass

conn.commit()

# Criar tabela de controle
print("\nCriando etl_controle...")
cursor.execute("""
CREATE TABLE etl_controle (
    nome_processo VARCHAR(50) PRIMARY KEY,
    ultima_execucao DATETIME,
    ultimo_registro DATETIME,
    registros_inseridos INT DEFAULT 0,
    registros_atualizados INT DEFAULT 0,
    status VARCHAR(20),
    mensagem TEXT
)
""")

# Criar dim_tempo
print("Criando dim_tempo...")
cursor.execute("""
CREATE TABLE dim_tempo (
    sk_tempo INT IDENTITY(1,1) PRIMARY KEY,
    data_completa DATE,
    ano INT,
    trimestre INT,
    mes INT,
    nome_mes VARCHAR(20),
    semana_ano INT,
    dia INT,
    dia_semana VARCHAR(10),
    eh_fim_semana BIT
)
""")

# Criar dim_produto com tamanhos corrigidos
print("Criando dim_produto...")
cursor.execute("""
CREATE TABLE dim_produto (
    sk_produto INT IDENTITY(1,1) PRIMARY KEY,
    nk_produto INT,
    nome_produto NVARCHAR(100),
    numero_produto NVARCHAR(50),
    cor NVARCHAR(30),
    tamanho NVARCHAR(10),
    peso DECIMAL(10,4),
    preco_lista DECIMAL(19,4),
    custo_padrao DECIMAL(19,4),
    subcategoria NVARCHAR(100),
    categoria NVARCHAR(100),
    classe NVARCHAR(10),
    linha_produto NVARCHAR(10),
    data_inicio DATE,
    data_fim DATE,
    registro_atual BIT
)
""")

# Criar dim_cliente
print("Criando dim_cliente...")
cursor.execute("""
CREATE TABLE dim_cliente (
    sk_cliente INT IDENTITY(1,1) PRIMARY KEY,
    nk_cliente INT,
    nome_completo NVARCHAR(200),
    tipo_cliente NVARCHAR(20),
    nome_loja NVARCHAR(100),
    email NVARCHAR(100),
    telefone NVARCHAR(25),
    territorio NVARCHAR(100),
    regiao NVARCHAR(100),
    pais NVARCHAR(10),
    estado NVARCHAR(100),
    cidade NVARCHAR(100),
    cep NVARCHAR(20),
    data_inicio DATE,
    data_fim DATE,
    registro_atual BIT
)
""")

# Criar dim_vendedor
print("Criando dim_vendedor...")
cursor.execute("""
CREATE TABLE dim_vendedor (
    sk_vendedor INT IDENTITY(1,1) PRIMARY KEY,
    nk_vendedor INT,
    nome_completo NVARCHAR(200),
    cargo NVARCHAR(100),
    territorio NVARCHAR(100),
    regiao NVARCHAR(100),
    pais NVARCHAR(10),
    cota_anual DECIMAL(19,4),
    bonus_ytd DECIMAL(19,4),
    comissao_pct DECIMAL(5,4),
    data_inicio DATE,
    data_fim DATE,
    registro_atual BIT
)
""")

# Criar dim_territorio
print("Criando dim_territorio...")
cursor.execute("""
CREATE TABLE dim_territorio (
    sk_territorio INT IDENTITY(1,1) PRIMARY KEY,
    nk_territorio INT,
    nome_territorio NVARCHAR(100),
    pais NVARCHAR(10),
    grupo NVARCHAR(100)
)
""")

# Criar dim_promocao
print("Criando dim_promocao...")
cursor.execute("""
CREATE TABLE dim_promocao (
    sk_promocao INT IDENTITY(1,1) PRIMARY KEY,
    nk_promocao INT,
    descricao NVARCHAR(255),
    tipo_desconto NVARCHAR(100),
    categoria NVARCHAR(100),
    percentual_desconto DECIMAL(5,4),
    quantidade_min INT,
    quantidade_max INT,
    data_inicio DATETIME,
    data_fim DATETIME
)
""")

# Criar fato_vendas
print("Criando fato_vendas...")
cursor.execute("""
CREATE TABLE fato_vendas (
    sk_venda INT IDENTITY(1,1) PRIMARY KEY,
    nk_pedido INT,
    nk_detalhe INT,
    sk_tempo INT FOREIGN KEY REFERENCES dim_tempo(sk_tempo),
    sk_produto INT FOREIGN KEY REFERENCES dim_produto(sk_produto),
    sk_cliente INT FOREIGN KEY REFERENCES dim_cliente(sk_cliente),
    sk_vendedor INT FOREIGN KEY REFERENCES dim_vendedor(sk_vendedor),
    sk_territorio INT FOREIGN KEY REFERENCES dim_territorio(sk_territorio),
    sk_promocao INT FOREIGN KEY REFERENCES dim_promocao(sk_promocao),
    quantidade INT,
    preco_unitario DECIMAL(19,4),
    desconto_unitario DECIMAL(19,4),
    custo_padrao DECIMAL(19,4),
    receita_bruta DECIMAL(19,4),
    receita_liquida DECIMAL(19,4),
    custo_total DECIMAL(19,4),
    lucro_bruto DECIMAL(19,4),
    margem_percentual DECIMAL(10,4),
    frete_rateado DECIMAL(19,4),
    imposto_rateado DECIMAL(19,4),
    data_atualizacao DATETIME
)
""")

conn.commit()
print("\n✅ Todas as tabelas foram recriadas com os tamanhos corretos!")
conn.close()