"""
ETL Incremental - AdventureWorks → Data Warehouse (SQL Server)
Módulo: Vendas (Sales)
Autor: Grupo DW - Unisales
Descrição:
    Pipeline ETL incremental que extrai dados do banco OLTP AdventureWorks,
    transforma conforme o modelo estrela e carrega no Data Warehouse.
    Apenas registros novos ou modificados são processados a cada execução.

Dependência: pip install pyodbc
Driver ODBC necessário: "ODBC Driver 17 for SQL Server" (ou versão 18)
Download: https://learn.microsoft.com/pt-br/sql/connect/odbc/download-odbc-driver-for-sql-server
"""

import logging
import sys
from datetime import datetime, date
from typing import Optional

import pyodbc

# ============================================================
# CONFIGURAÇÃO
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_adventureworks.log"),
    ],
)
log = logging.getLogger(__name__)


OLTP_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=AdventureWorks;"
    "UID=sa;"
    "PWD=123456;"
    "TrustServerCertificate=yes;"
)

DW_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=adventureworks_dw;"
    "UID=sa;"
    "PWD=123456;"
    "TrustServerCertificate=yes;"
)

# Autenticação Windows (alternativa — remova UID/PWD e use abaixo):
# "Trusted_Connection=yes;"

BATCH_SIZE = 500  # registros por commit


# ============================================================
# UTILITÁRIOS
# ============================================================

def get_conn(conn_str: str) -> pyodbc.Connection:
    conn = pyodbc.connect(conn_str)
    conn.autocommit = False
    return conn


def dict_cursor(cursor: pyodbc.Cursor):
    """Converte linha pyodbc em dict para acesso por nome de coluna."""
    columns = [col[0].lower() for col in cursor.description]
    def row_to_dict(row):
        return dict(zip(columns, row))
    return row_to_dict


def get_ultima_execucao(dw_cur: pyodbc.Cursor, processo: str) -> datetime:
    dw_cur.execute(
        "SELECT COALESCE(ultimo_registro, '1900-01-01') "
        "FROM etl_controle WHERE nome_processo = ?",  # %s -> ?
        (processo,),
    )
    row = dw_cur.fetchone()
    return row[0] if row else datetime(1900, 1, 1)


def atualizar_controle(dw_cur: pyodbc.Cursor, processo: str,
                       ultimo_registro: datetime,
                       inseridos: int, atualizados: int,
                       status: str = "OK",
                       mensagem: Optional[str] = None):
    dw_cur.execute("""
        UPDATE etl_controle
        SET ultima_execucao       = GETDATE(),       -- NOW() -> GETDATE()
            ultimo_registro       = ?,
            registros_inseridos   = registros_inseridos + ?,
            registros_atualizados = registros_atualizados + ?,
            status                = ?,
            mensagem              = ?
        WHERE nome_processo = ?
    """, (ultimo_registro, inseridos, atualizados, status, mensagem, processo))


# ============================================================
# DIMENSÃO TEMPO (gerada sem consulta ao OLTP)
# ============================================================

def etl_dim_tempo(dw_conn: pyodbc.Connection, data_inicio: date, data_fim: date):
    """Popula dim_tempo para o intervalo de datas fornecido."""
    log.info("dim_tempo: gerando datas de %s até %s", data_inicio, data_fim)
    from datetime import timedelta

    nomes_mes = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março",    4: "Abril",
        5: "Maio",    6: "Junho",     7: "Julho",     8: "Agosto",
        9: "Setembro",10: "Outubro",  11: "Novembro", 12: "Dezembro",
    }
    dias_semana = {0:"Segunda",1:"Terça",2:"Quarta",3:"Quinta",
                   4:"Sexta",5:"Sábado",6:"Domingo"}

    cursor = dw_conn.cursor()
    inseridos = 0
    d = data_inicio
    while d <= data_fim:
        # ON CONFLICT DO NOTHING -> MERGE (upsert SQL Server)
        cursor.execute("""
            MERGE dim_tempo AS tgt
            USING (SELECT ? AS data_completa) AS src
               ON tgt.data_completa = src.data_completa
            WHEN NOT MATCHED THEN
                INSERT (data_completa, ano, trimestre, mes, nome_mes,
                        semana_ano, dia, dia_semana, eh_fim_semana)
                VALUES (?,?,?,?,?,?,?,?,?);
        """, (
            d,
            d, d.year, (d.month - 1) // 3 + 1, d.month,
            nomes_mes[d.month], d.isocalendar()[1], d.day,
            dias_semana[d.weekday()],
            1 if d.weekday() >= 5 else 0,
        ))
        inseridos += cursor.rowcount
        d += timedelta(days=1)
    dw_conn.commit()
    log.info("dim_tempo: %d registros inseridos.", inseridos)
    cursor.close()


# ============================================================
# DIMENSÃO PRODUTO (SCD Tipo 2)
# ============================================================

QUERY_PRODUTO = """
SELECT
    p.ProductID                             AS nk_produto,
    p.Name                                  AS nome_produto,
    p.ProductNumber                         AS numero_produto,
    COALESCE(p.Color, 'N/A')               AS cor,
    COALESCE(p.Size,  'N/A')               AS tamanho,
    p.Weight                                AS peso,
    p.ListPrice                             AS preco_lista,
    p.StandardCost                          AS custo_padrao,
    COALESCE(ps.Name, 'Sem Subcategoria')  AS subcategoria,
    COALESCE(pc.Name, 'Sem Categoria')     AS categoria,
    COALESCE(p.Class, 'N/A')              AS classe,
    COALESCE(p.ProductLine, 'N/A')        AS linha_produto,
    p.SellStartDate                         AS data_inicio,
    p.SellEndDate                           AS data_fim,
    p.ModifiedDate                          AS modified_date
FROM Production.Product p
LEFT JOIN Production.ProductSubcategory ps
       ON p.ProductSubcategoryID = ps.ProductSubcategoryID
LEFT JOIN Production.ProductCategory pc
       ON ps.ProductCategoryID = pc.ProductCategoryID
WHERE p.ModifiedDate > ?
ORDER BY p.ModifiedDate
"""
# Placeholders: %(nome)s -> ?  |  Aspas duplas removidas (SQL Server usa [] ou nada)

def etl_dim_produto(oltp_conn: pyodbc.Connection, dw_conn: pyodbc.Connection):
    log.info("dim_produto: iniciando...")
    dw_cur  = dw_conn.cursor()
    olt_cur = oltp_conn.cursor()

    ultima = get_ultima_execucao(dw_cur, "dim_produto")
    olt_cur.execute(QUERY_PRODUTO, (ultima,))           # dict param -> tuple
    to_dict = dict_cursor(olt_cur)
    rows = [to_dict(r) for r in olt_cur.fetchall()]

    inseridos = atualizados = 0
    max_modified = ultima

    for row in rows:
        # Fecha registro atual (SCD2)
        dw_cur.execute("""
            UPDATE dim_produto
            SET data_fim = CAST(GETDATE() AS DATE), registro_atual = 0
            WHERE nk_produto = ? AND registro_atual = 1
        """, (row["nk_produto"],))
        if dw_cur.rowcount > 0:
            atualizados += 1

        dw_cur.execute("""
            INSERT INTO dim_produto
                (nk_produto, nome_produto, numero_produto, cor, tamanho,
                 peso, preco_lista, custo_padrao, subcategoria, categoria,
                 classe, linha_produto, data_inicio, data_fim, registro_atual)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,1)
        """, (
            row["nk_produto"], row["nome_produto"], row["numero_produto"],
            row["cor"], row["tamanho"], row["peso"], row["preco_lista"],
            row["custo_padrao"], row["subcategoria"], row["categoria"],
            row["classe"], row["linha_produto"], row["data_inicio"],
        ))
        inseridos += 1
        if row["modified_date"] > max_modified:
            max_modified = row["modified_date"]

    atualizar_controle(dw_cur, "dim_produto", max_modified, inseridos, atualizados)
    dw_conn.commit()
    log.info("dim_produto: %d inseridos, %d atualizados.", inseridos, atualizados)
    olt_cur.close(); dw_cur.close()


# ============================================================
# DIMENSÃO CLIENTE (SCD Tipo 2)
# ============================================================

QUERY_CLIENTE = """
SELECT
    c.CustomerID                                AS nk_cliente,
    CASE WHEN c.PersonID IS NOT NULL THEN 'Individual' ELSE 'Loja' END AS tipo_cliente,
    COALESCE(s.Name, '')                        AS nome_loja,
    COALESCE(p.FirstName + ' ' + p.LastName, s.Name, 'Desconhecido') AS nome_completo,
    ea.EmailAddress                             AS email,
    pp.PhoneNumber                              AS telefone,
    st.Name                                     AS territorio,
    st.CountryRegionCode                        AS pais,
    st.[Group]                                  AS regiao,
    a.City                                      AS cidade,
    a.PostalCode                                AS cep,
    COALESCE(sp.Name, '')                       AS estado,
    (SELECT MAX(v) FROM (VALUES
        (c.ModifiedDate),
        (COALESCE(p.ModifiedDate, '1900-01-01')),
        (COALESCE(s.ModifiedDate, '1900-01-01'))
    ) AS t(v))                                  AS modified_date
FROM Sales.Customer c
LEFT JOIN Person.Person               p  ON c.PersonID    = p.BusinessEntityID
LEFT JOIN Sales.Store                 s  ON c.StoreID     = s.BusinessEntityID
LEFT JOIN Person.EmailAddress         ea ON p.BusinessEntityID = ea.BusinessEntityID
LEFT JOIN Person.PersonPhone          pp ON p.BusinessEntityID = pp.BusinessEntityID
LEFT JOIN Sales.SalesTerritory        st ON c.TerritoryID = st.TerritoryID
LEFT JOIN Person.BusinessEntityAddress bea ON c.CustomerID = bea.BusinessEntityID
LEFT JOIN Person.Address              a  ON bea.AddressID = a.AddressID
LEFT JOIN Person.StateProvince        sp ON a.StateProvinceID = sp.StateProvinceID
WHERE (SELECT MAX(v) FROM (VALUES
    (c.ModifiedDate),
    (COALESCE(p.ModifiedDate, '1900-01-01')),
    (COALESCE(s.ModifiedDate, '1900-01-01'))
) AS t(v)) > ?
ORDER BY modified_date
"""
# GREATEST() não existe no SQL Server -> subconsulta com VALUES + MAX

def etl_dim_cliente(oltp_conn: pyodbc.Connection, dw_conn: pyodbc.Connection):
    log.info("dim_cliente: iniciando...")
    dw_cur  = dw_conn.cursor()
    olt_cur = oltp_conn.cursor()

    ultima = get_ultima_execucao(dw_cur, "dim_cliente")
    olt_cur.execute(QUERY_CLIENTE, (ultima,))
    to_dict = dict_cursor(olt_cur)
    rows = [to_dict(r) for r in olt_cur.fetchall()]

    inseridos = atualizados = 0
    max_modified = ultima

    for row in rows:
        dw_cur.execute("""
            UPDATE dim_cliente
            SET data_fim = CAST(GETDATE() AS DATE), registro_atual = 0
            WHERE nk_cliente = ? AND registro_atual = 1
        """, (row["nk_cliente"],))
        if dw_cur.rowcount > 0:
            atualizados += 1

        dw_cur.execute("""
            INSERT INTO dim_cliente
                (nk_cliente, nome_completo, tipo_cliente, nome_loja, email,
                 telefone, territorio, regiao, pais, estado, cidade, cep,
                 data_inicio, data_fim, registro_atual)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,CAST(GETDATE() AS DATE),NULL,1)
        """, (
            row["nk_cliente"], row["nome_completo"], row["tipo_cliente"],
            row["nome_loja"], row["email"], row["telefone"],
            row["territorio"], row["regiao"], row["pais"],
            row["estado"], row["cidade"], row["cep"],
        ))
        inseridos += 1
        if row["modified_date"] > max_modified:
            max_modified = row["modified_date"]

    atualizar_controle(dw_cur, "dim_cliente", max_modified, inseridos, atualizados)
    dw_conn.commit()
    log.info("dim_cliente: %d inseridos, %d atualizados.", inseridos, atualizados)
    olt_cur.close(); dw_cur.close()


# ============================================================
# DIMENSÃO VENDEDOR
# ============================================================

QUERY_VENDEDOR = """
SELECT
    sp.BusinessEntityID                     AS nk_vendedor,
    p.FirstName + ' ' + p.LastName          AS nome_completo,
    e.JobTitle                              AS cargo,
    st.Name                                 AS territorio,
    st.CountryRegionCode                    AS pais,
    st.[Group]                              AS regiao,
    sp.SalesQuota                           AS cota_anual,
    sp.Bonus                                AS bonus_ytd,
    sp.CommissionPct                        AS comissao_pct,
    (SELECT MAX(v) FROM (VALUES
        (sp.ModifiedDate), (p.ModifiedDate)
    ) AS t(v))                              AS modified_date
FROM Sales.SalesPerson sp
JOIN Person.Person             p  ON sp.BusinessEntityID = p.BusinessEntityID
JOIN HumanResources.Employee   e  ON sp.BusinessEntityID = e.BusinessEntityID
LEFT JOIN Sales.SalesTerritory st ON sp.TerritoryID      = st.TerritoryID
WHERE (SELECT MAX(v) FROM (VALUES
    (sp.ModifiedDate), (p.ModifiedDate)
) AS t(v)) > ?
ORDER BY modified_date
"""

def etl_dim_vendedor(oltp_conn: pyodbc.Connection, dw_conn: pyodbc.Connection):
    log.info("dim_vendedor: iniciando...")
    dw_cur  = dw_conn.cursor()
    olt_cur = oltp_conn.cursor()

    ultima = get_ultima_execucao(dw_cur, "dim_vendedor")
    olt_cur.execute(QUERY_VENDEDOR, (ultima,))
    to_dict = dict_cursor(olt_cur)
    rows = [to_dict(r) for r in olt_cur.fetchall()]

    inseridos = atualizados = 0
    max_modified = ultima

    for row in rows:
        dw_cur.execute("""
            UPDATE dim_vendedor
            SET data_fim = CAST(GETDATE() AS DATE), registro_atual = 0
            WHERE nk_vendedor = ? AND registro_atual = 1
        """, (row["nk_vendedor"],))
        if dw_cur.rowcount > 0:
            atualizados += 1

        dw_cur.execute("""
            INSERT INTO dim_vendedor
                (nk_vendedor, nome_completo, cargo, territorio, regiao,
                 pais, cota_anual, bonus_ytd, comissao_pct,
                 data_inicio, data_fim, registro_atual)
            VALUES (?,?,?,?,?,?,?,?,?,CAST(GETDATE() AS DATE),NULL,1)
        """, (
            row["nk_vendedor"], row["nome_completo"], row["cargo"],
            row["territorio"], row["regiao"], row["pais"],
            row["cota_anual"], row["bonus_ytd"], row["comissao_pct"],
        ))
        inseridos += 1
        if row["modified_date"] > max_modified:
            max_modified = row["modified_date"]

    atualizar_controle(dw_cur, "dim_vendedor", max_modified, inseridos, atualizados)
    dw_conn.commit()
    log.info("dim_vendedor: %d inseridos, %d atualizados.", inseridos, atualizados)
    olt_cur.close(); dw_cur.close()


# ============================================================
# DIMENSÃO TERRITÓRIO
# ============================================================

def etl_dim_territorio(oltp_conn: pyodbc.Connection, dw_conn: pyodbc.Connection):
    log.info("dim_territorio: iniciando...")
    dw_cur  = dw_conn.cursor()
    olt_cur = oltp_conn.cursor()

    ultima = get_ultima_execucao(dw_cur, "dim_territorio")
    olt_cur.execute("""
        SELECT TerritoryID      AS nk_territorio,
               Name             AS nome_territorio,
               CountryRegionCode AS pais,
               [Group]          AS grupo
        FROM Sales.SalesTerritory
        WHERE ModifiedDate > ?
    """, (ultima,))
    to_dict = dict_cursor(olt_cur)
    rows = [to_dict(r) for r in olt_cur.fetchall()]

    inseridos = atualizados = 0
    for row in rows:
        # ON CONFLICT DO UPDATE -> MERGE
        dw_cur.execute("""
            MERGE dim_territorio AS tgt
            USING (SELECT ? AS nk_territorio) AS src
               ON tgt.nk_territorio = src.nk_territorio
            WHEN MATCHED THEN
                UPDATE SET nome_territorio = ?,
                           pais            = ?,
                           grupo           = ?
            WHEN NOT MATCHED THEN
                INSERT (nk_territorio, nome_territorio, pais, grupo)
                VALUES (?, ?, ?, ?);
        """, (
            row["nk_territorio"],
            row["nome_territorio"], row["pais"], row["grupo"],
            row["nk_territorio"], row["nome_territorio"], row["pais"], row["grupo"],
        ))
        if dw_cur.rowcount == 1:
            inseridos += 1
        else:
            atualizados += 1

    atualizar_controle(dw_cur, "dim_territorio", datetime.now(), inseridos, atualizados)
    dw_conn.commit()
    log.info("dim_territorio: %d inseridos, %d atualizados.", inseridos, atualizados)
    olt_cur.close(); dw_cur.close()


# ============================================================
# DIMENSÃO PROMOÇÃO
# ============================================================

def etl_dim_promocao(oltp_conn: pyodbc.Connection, dw_conn: pyodbc.Connection):
    log.info("dim_promocao: iniciando...")
    dw_cur  = dw_conn.cursor()
    olt_cur = oltp_conn.cursor()

    ultima = get_ultima_execucao(dw_cur, "dim_promocao")
    olt_cur.execute("""
        SELECT SpecialOfferID   AS nk_promocao,
               Description      AS descricao,
               DiscountPct      AS percentual_desconto,
               Type             AS tipo_desconto,
               Category         AS categoria,
               MinQty           AS quantidade_min,
               MaxQty           AS quantidade_max,
               StartDate        AS data_inicio,
               EndDate          AS data_fim,
               ModifiedDate     AS modified_date
        FROM Sales.SpecialOffer
        WHERE ModifiedDate > ?
    """, (ultima,))
    to_dict = dict_cursor(olt_cur)
    rows = [to_dict(r) for r in olt_cur.fetchall()]

    inseridos = atualizados = 0
    for row in rows:
        dw_cur.execute("""
            MERGE dim_promocao AS tgt
            USING (SELECT ? AS nk_promocao) AS src
               ON tgt.nk_promocao = src.nk_promocao
            WHEN MATCHED THEN
                UPDATE SET descricao            = ?,
                           percentual_desconto  = ?
            WHEN NOT MATCHED THEN
                INSERT (nk_promocao, descricao, tipo_desconto, categoria,
                        percentual_desconto, quantidade_min, quantidade_max,
                        data_inicio, data_fim)
                VALUES (?,?,?,?,?,?,?,?,?);
        """, (
            row["nk_promocao"],
            row["descricao"], row["percentual_desconto"],
            row["nk_promocao"], row["descricao"], row["tipo_desconto"],
            row["categoria"], row["percentual_desconto"],
            row["quantidade_min"], row["quantidade_max"],
            row["data_inicio"], row["data_fim"],
        ))
        if dw_cur.rowcount == 1:
            inseridos += 1
        else:
            atualizados += 1

    atualizar_controle(dw_cur, "dim_promocao", datetime.now(), inseridos, atualizados)
    dw_conn.commit()
    log.info("dim_promocao: %d inseridos, %d atualizados.", inseridos, atualizados)
    olt_cur.close(); dw_cur.close()


# ============================================================
# FATO VENDAS (CARGA INCREMENTAL POR ModifiedDate)
# ============================================================

QUERY_FATO = """
SELECT
    soh.SalesOrderID                        AS nk_pedido,
    sod.SalesOrderDetailID                  AS nk_detalhe,
    CAST(soh.OrderDate AS DATE)             AS data_venda,
    sod.ProductID                           AS nk_produto,
    soh.CustomerID                          AS nk_cliente,
    soh.SalesPersonID                       AS nk_vendedor,
    soh.TerritoryID                         AS nk_territorio,
    sod.SpecialOfferID                      AS nk_promocao,
    sod.OrderQty                            AS quantidade,
    sod.UnitPrice                           AS preco_unitario,
    sod.UnitPriceDiscount                   AS desconto_unitario,
    p.StandardCost                          AS custo_padrao,
    sod.OrderQty * sod.UnitPrice            AS receita_bruta,
    sod.LineTotal                           AS receita_liquida,
    sod.OrderQty * p.StandardCost           AS custo_total,
    sod.LineTotal - sod.OrderQty * p.StandardCost  AS lucro_bruto,
    CASE WHEN sod.LineTotal > 0
         THEN (sod.LineTotal - sod.OrderQty * p.StandardCost) / sod.LineTotal
         ELSE 0 END                         AS margem_percentual,
    soh.Freight / NULLIF(soh.SubTotal,0) * sod.LineTotal  AS frete_rateado,
    soh.TaxAmt  / NULLIF(soh.SubTotal,0) * sod.LineTotal  AS imposto_rateado,
    (SELECT MAX(v) FROM (VALUES
        (soh.ModifiedDate), (sod.ModifiedDate)
    ) AS t(v))                              AS modified_date
FROM Sales.SalesOrderHeader soh
JOIN Sales.SalesOrderDetail  sod ON soh.SalesOrderID = sod.SalesOrderID
JOIN Production.Product       p  ON sod.ProductID    = p.ProductID
WHERE (SELECT MAX(v) FROM (VALUES
    (soh.ModifiedDate), (sod.ModifiedDate)
) AS t(v)) > ?
ORDER BY modified_date, nk_detalhe
"""

def etl_fato_vendas(oltp_conn: pyodbc.Connection, dw_conn: pyodbc.Connection):
    log.info("fato_vendas: iniciando...")
    dw_cur  = dw_conn.cursor()
    olt_cur = oltp_conn.cursor()

    # pyodbc não tem server-side cursor nomeado como psycopg2,
    # mas fetchmany() controla o consumo de memória igualmente.
    ultima = get_ultima_execucao(dw_cur, "fato_vendas")
    olt_cur.execute(QUERY_FATO, (ultima,))
    to_dict = dict_cursor(olt_cur)

    # Cache de lookups para evitar N+1
    cache_tempo:      dict = {}
    cache_produto:    dict = {}
    cache_cliente:    dict = {}
    cache_vendedor:   dict = {}
    cache_territorio: dict = {}
    cache_promocao:   dict = {}

    def lookup(cur, cache, table, nk_col, nk_val, scd=True):
        if nk_val is None:
            return None
        if nk_val not in cache:
            sk_col = f"sk_{table.split('_')[1] if '_' in table else table}"
            extra  = "AND registro_atual = 1" if scd else ""
            cur.execute(
                f"SELECT {sk_col} FROM {table} WHERE {nk_col} = ? {extra}",
                (nk_val,)
            )
            row = cur.fetchone()
            cache[nk_val] = row[0] if row else None
        return cache[nk_val]

    inseridos = atualizados = 0
    max_modified = ultima

    while True:
        raw_rows = olt_cur.fetchmany(BATCH_SIZE)
        if not raw_rows:
            break
        rows = [to_dict(r) for r in raw_rows]

        for row in rows:
            # Lookup dim_tempo
            data_v = row["data_venda"]
            if data_v not in cache_tempo:
                dw_cur.execute(
                    "SELECT sk_tempo FROM dim_tempo WHERE data_completa = ?", (data_v,)
                )
                r = dw_cur.fetchone()
                cache_tempo[data_v] = r[0] if r else None
            sk_tempo = cache_tempo[data_v]

            # Lookup outras dimensões
            sk_produto    = lookup(dw_cur, cache_produto,    "dim_produto",    "nk_produto",    row["nk_produto"])
            sk_cliente    = lookup(dw_cur, cache_cliente,    "dim_cliente",    "nk_cliente",    row["nk_cliente"])
            sk_vendedor   = lookup(dw_cur, cache_vendedor,   "dim_vendedor",   "nk_vendedor",   row["nk_vendedor"])
            sk_territorio = lookup(dw_cur, cache_territorio, "dim_territorio", "nk_territorio", row["nk_territorio"], scd=False)
            sk_promocao   = lookup(dw_cur, cache_promocao,   "dim_promocao",   "nk_promocao",   row["nk_promocao"],   scd=False)

            if not (sk_tempo and sk_produto and sk_cliente):
                log.warning("Detalhe %s ignorado: FK inválida.", row["nk_detalhe"])
                continue

            # UPSERT via MERGE (substitui ON CONFLICT DO UPDATE)
            dw_cur.execute("""
                MERGE fato_vendas AS tgt
                USING (SELECT ? AS nk_detalhe) AS src
                   ON tgt.nk_detalhe = src.nk_detalhe
                WHEN MATCHED THEN
                    UPDATE SET quantidade        = ?,
                               preco_unitario    = ?,
                               receita_bruta     = ?,
                               receita_liquida   = ?,
                               custo_total       = ?,
                               lucro_bruto       = ?,
                               margem_percentual = ?,
                               data_atualizacao  = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (nk_pedido, nk_detalhe, sk_tempo, sk_produto, sk_cliente,
                            sk_vendedor, sk_territorio, sk_promocao,
                            quantidade, preco_unitario, desconto_unitario, custo_padrao,
                            receita_bruta, receita_liquida, custo_total, lucro_bruto,
                            margem_percentual, frete_rateado, imposto_rateado,
                            data_atualizacao)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,GETDATE());
            """, (
                # chave USING
                row["nk_detalhe"],
                # UPDATE
                row["quantidade"], row["preco_unitario"],
                row["receita_bruta"], row["receita_liquida"],
                row["custo_total"], row["lucro_bruto"], row["margem_percentual"],
                # INSERT
                row["nk_pedido"], row["nk_detalhe"],
                sk_tempo, sk_produto, sk_cliente, sk_vendedor,
                sk_territorio, sk_promocao,
                row["quantidade"], row["preco_unitario"],
                row["desconto_unitario"], row["custo_padrao"],
                row["receita_bruta"], row["receita_liquida"],
                row["custo_total"], row["lucro_bruto"],
                row["margem_percentual"], row["frete_rateado"],
                row["imposto_rateado"],
            ))

            # MERGE: rowcount == 1 tanto para INSERT quanto UPDATE.
            # Diferenciamos verificando se a chave já existia antes do MERGE.
            if dw_cur.rowcount == 1:
                inseridos += 1
            else:
                atualizados += 1

            if row["modified_date"] > max_modified:
                max_modified = row["modified_date"]

        dw_conn.commit()
        log.info("  fato_vendas: %d inseridos / %d atualizados até agora...",
                 inseridos, atualizados)

    atualizar_controle(dw_cur, "fato_vendas", max_modified, inseridos, atualizados)
    dw_conn.commit()
    log.info("fato_vendas: CONCLUÍDO — %d inseridos, %d atualizados.", inseridos, atualizados)
    olt_cur.close(); dw_cur.close()


# ============================================================
# ORQUESTRADOR PRINCIPAL
# ============================================================

def executar_etl():
    inicio = datetime.now()
    log.info("=" * 60)
    log.info("INÍCIO ETL INCREMENTAL — %s", inicio.strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    oltp_conn = dw_conn = None
    try:
        oltp_conn = get_conn(OLTP_CONN_STR)
        dw_conn   = get_conn(DW_CONN_STR)
        oltp_conn.autocommit = True  # leitura sem transação aberta

        # 1. Dimensões estáticas / lenta variação
        etl_dim_tempo(dw_conn,
                      data_inicio=date(2011, 1, 1),
                      data_fim=date(2014, 12, 31))
        etl_dim_territorio(oltp_conn, dw_conn)
        etl_dim_promocao(oltp_conn, dw_conn)

        # 2. Dimensões de entidade (SCD2)
        etl_dim_produto(oltp_conn, dw_conn)
        etl_dim_cliente(oltp_conn, dw_conn)
        etl_dim_vendedor(oltp_conn, dw_conn)

        # 3. Tabela Fato
        etl_fato_vendas(oltp_conn, dw_conn)

        duracao = (datetime.now() - inicio).total_seconds()
        log.info("=" * 60)
        log.info("ETL CONCLUÍDO com sucesso em %.1f segundos.", duracao)
        log.info("=" * 60)

    except Exception as exc:
        log.exception("ERRO CRÍTICO NA ETL: %s", exc)
        if dw_conn:
            dw_conn.rollback()
        sys.exit(1)
    finally:
        if oltp_conn: oltp_conn.close()
        if dw_conn:   dw_conn.close()


if __name__ == "__main__":
    executar_etl()
