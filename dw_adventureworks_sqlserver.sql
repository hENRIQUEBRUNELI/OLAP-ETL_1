-- ============================================================
-- DATA WAREHOUSE - AdventureWorks Sales
-- Modelo Estrela (Star Schema) - SQL Server
-- VERSÃO COM CAMPOS AJUSTADOS PARA O ETL PYTHON
-- ============================================================

-- ============================================================
-- DIMENSÕES
-- ============================================================

CREATE TABLE dim_tempo (
    sk_tempo        INT IDENTITY(1,1) PRIMARY KEY,
    data_completa   DATE NOT NULL,
    ano             INT NOT NULL,
    trimestre       INT NOT NULL,
    mes             INT NOT NULL,
    nome_mes        VARCHAR(20) NOT NULL,
    semana_ano      INT NOT NULL,
    dia             INT NOT NULL,
    dia_semana      VARCHAR(10) NOT NULL,  -- Reduzido para comportar dias em português
    eh_fim_semana   BIT NOT NULL,
    CONSTRAINT uq_dim_tempo_data UNIQUE (data_completa)
);

CREATE TABLE dim_produto (
    sk_produto          INT IDENTITY(1,1) PRIMARY KEY,
    nk_produto          INT NOT NULL,  -- ProductID (OLTP)
    nome_produto        NVARCHAR(100) NOT NULL,  -- Aumentado e usando NVARCHAR
    numero_produto      NVARCHAR(50),  -- Aumentado
    cor                 NVARCHAR(30),  -- Aumentado para 'N/A'
    tamanho             NVARCHAR(10),  -- Aumentado
    peso                DECIMAL(10,4),  -- Mais precisão
    preco_lista         DECIMAL(19,4),  -- Mais precisão
    custo_padrao        DECIMAL(19,4),  -- Mais precisão
    subcategoria        NVARCHAR(100),
    categoria           NVARCHAR(100),
    classe              NVARCHAR(10),  -- Aumentado de 2 para 10 (comporta 'N/A')
    linha_produto       NVARCHAR(10),  -- Aumentado de 2 para 10 (comporta 'N/A')
    data_inicio         DATE,
    data_fim            DATE,
    registro_atual      BIT DEFAULT 1,
    CONSTRAINT uq_dim_produto_nk UNIQUE (nk_produto, registro_atual)
);

CREATE TABLE dim_cliente (
    sk_cliente          INT IDENTITY(1,1) PRIMARY KEY,
    nk_cliente          INT NOT NULL,  -- CustomerID (OLTP)
    nome_completo       NVARCHAR(200),
    tipo_cliente        NVARCHAR(20),   -- 'Individual' ou 'Loja'
    nome_loja           NVARCHAR(100),
    email               NVARCHAR(100),
    telefone            NVARCHAR(25),
    territorio          NVARCHAR(100),
    regiao              NVARCHAR(100),
    pais                NVARCHAR(10),  -- Código do país (ex: 'US', 'BR')
    estado              NVARCHAR(100),
    cidade              NVARCHAR(100),
    cep                 NVARCHAR(20),
    data_inicio         DATE,
    data_fim            DATE,
    registro_atual      BIT DEFAULT 1,
    CONSTRAINT uq_dim_cliente_nk UNIQUE (nk_cliente, registro_atual)
);

CREATE TABLE dim_vendedor (
    sk_vendedor         INT IDENTITY(1,1) PRIMARY KEY,
    nk_vendedor         INT NOT NULL,  -- SalesPersonID (OLTP)
    nome_completo       NVARCHAR(200),
    cargo               NVARCHAR(100),
    territorio          NVARCHAR(100),
    regiao              NVARCHAR(100),
    pais                NVARCHAR(10),
    cota_anual          DECIMAL(19,4),
    bonus_ytd           DECIMAL(19,4),
    comissao_pct        DECIMAL(5,4),
    data_inicio         DATE,
    data_fim            DATE,
    registro_atual      BIT DEFAULT 1,
    CONSTRAINT uq_dim_vendedor_nk UNIQUE (nk_vendedor, registro_atual)
);

CREATE TABLE dim_territorio (
    sk_territorio       INT IDENTITY(1,1) PRIMARY KEY,
    nk_territorio       INT NOT NULL,  -- TerritoryID (OLTP)
    nome_territorio     NVARCHAR(100) NOT NULL,
    pais                NVARCHAR(10),
    grupo               NVARCHAR(100),
    CONSTRAINT uq_dim_territorio_nk UNIQUE (nk_territorio)
);

CREATE TABLE dim_promocao (
    sk_promocao         INT IDENTITY(1,1) PRIMARY KEY,
    nk_promocao         INT NOT NULL,  -- SpecialOfferID (OLTP)
    descricao           NVARCHAR(255),
    tipo_desconto       NVARCHAR(100),
    categoria           NVARCHAR(100),
    percentual_desconto DECIMAL(5,4),
    quantidade_min      INT,
    quantidade_max      INT,
    data_inicio         DATETIME,  -- Mudado para DATETIME para compatibilidade
    data_fim            DATETIME,  -- Mudado para DATETIME
    CONSTRAINT uq_dim_promocao_nk UNIQUE (nk_promocao)
);

-- ============================================================
-- FOTO PRINCIPAL
-- ============================================================

CREATE TABLE fato_vendas (
    sk_venda            INT IDENTITY(1,1) PRIMARY KEY,
    nk_pedido           INT NOT NULL,       -- SalesOrderID (OLTP)
    nk_detalhe          INT NOT NULL,       -- SalesOrderDetailID (OLTP)
    sk_tempo            INT NOT NULL REFERENCES dim_tempo(sk_tempo),
    sk_produto          INT NOT NULL REFERENCES dim_produto(sk_produto),
    sk_cliente          INT NOT NULL REFERENCES dim_cliente(sk_cliente),
    sk_vendedor         INT NULL REFERENCES dim_vendedor(sk_vendedor),
    sk_territorio       INT NULL REFERENCES dim_territorio(sk_territorio),
    sk_promocao         INT NULL REFERENCES dim_promocao(sk_promocao),

    -- Métricas
    quantidade          INT NOT NULL,
    preco_unitario      DECIMAL(19,4) NOT NULL,
    desconto_unitario   DECIMAL(19,4) NOT NULL DEFAULT 0,
    custo_padrao        DECIMAL(19,4),
    receita_bruta       DECIMAL(19,4) NOT NULL,
    receita_liquida     DECIMAL(19,4) NOT NULL,
    custo_total         DECIMAL(19,4),
    lucro_bruto         DECIMAL(19,4),
    margem_percentual   DECIMAL(10,4),
    frete_rateado       DECIMAL(19,4),
    imposto_rateado     DECIMAL(19,4),

    -- Controle ETL
    data_carga          DATETIME2 DEFAULT GETDATE(),
    data_atualizacao    DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT uq_fato_vendas_detalhe UNIQUE (nk_detalhe)
);

-- ============================================================
-- TABELA DE CONTROLE ETL INCREMENTAL
-- ============================================================

CREATE TABLE etl_controle (
    id                    INT IDENTITY(1,1) PRIMARY KEY,
    nome_processo         VARCHAR(50) NOT NULL,  -- Reduzido para 50
    ultima_execucao       DATETIME2,
    ultimo_registro       DATETIME2,
    registros_inseridos   INT DEFAULT 0,
    registros_atualizados INT DEFAULT 0,
    status                VARCHAR(20) DEFAULT 'OK',
    mensagem              TEXT,  -- Usando TEXT para compatibilidade
    CONSTRAINT uq_etl_controle_processo UNIQUE (nome_processo)
);

-- Inserir registros iniciais de controle
INSERT INTO etl_controle (nome_processo, ultima_execucao, ultimo_registro)
VALUES
    ('dim_tempo',      '1900-01-01', '1900-01-01'),
    ('dim_produto',    '1900-01-01', '1900-01-01'),
    ('dim_cliente',    '1900-01-01', '1900-01-01'),
    ('dim_vendedor',   '1900-01-01', '1900-01-01'),
    ('dim_territorio', '1900-01-01', '1900-01-01'),
    ('dim_promocao',   '1900-01-01', '1900-01-01'),
    ('fato_vendas',    '1900-01-01', '1900-01-01');

-- ============================================================
-- ÍNDICES DE PERFORMANCE
-- ============================================================

CREATE INDEX idx_fato_tempo      ON fato_vendas(sk_tempo);
CREATE INDEX idx_fato_produto    ON fato_vendas(sk_produto);
CREATE INDEX idx_fato_cliente    ON fato_vendas(sk_cliente);
CREATE INDEX idx_fato_vendedor   ON fato_vendas(sk_vendedor);
CREATE INDEX idx_fato_territorio ON fato_vendas(sk_territorio);
CREATE INDEX idx_fato_nk_pedido  ON fato_vendas(nk_pedido);
CREATE INDEX idx_fato_nk_detalhe  ON fato_vendas(nk_detalhe);

CREATE INDEX idx_dim_tempo_data  ON dim_tempo(data_completa);
CREATE INDEX idx_dim_tempo_ano   ON dim_tempo(ano, mes);
CREATE INDEX idx_dim_produto_nk  ON dim_produto(nk_produto, registro_atual);
CREATE INDEX idx_dim_cliente_nk  ON dim_cliente(nk_cliente, registro_atual);
CREATE INDEX idx_dim_vendedor_nk ON dim_vendedor(nk_vendedor, registro_atual);