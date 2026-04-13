-- ============================================================
-- CONSULTAS SQL - 10 KPIs DO DATA WAREHOUSE DE VENDAS
-- AdventureWorks - Star Schema - SQL Server
-- ============================================================

-- ============================================================
-- KPI 1: Receita Total de Vendas por Período
-- Descrição: Soma da receita líquida de vendas por ano e mês
-- ============================================================
SELECT
    t.ano,
    t.mes,
    t.nome_mes,
    SUM(f.receita_liquida)                          AS receita_total,
    SUM(f.receita_liquida) - LAG(SUM(f.receita_liquida))
        OVER (ORDER BY t.ano, t.mes)                AS variacao_periodo_anterior,
    ROUND(
        (SUM(f.receita_liquida) - LAG(SUM(f.receita_liquida))
            OVER (ORDER BY t.ano, t.mes))
        / NULLIF(LAG(SUM(f.receita_liquida))
            OVER (ORDER BY t.ano, t.mes), 0) * 100, 2
    )                                               AS variacao_pct
FROM fato_vendas f
JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
GROUP BY t.ano, t.mes, t.nome_mes
ORDER BY t.ano, t.mes;


-- ============================================================
-- KPI 2: Ticket Médio por Pedido
-- Descrição: Valor médio de cada pedido de venda
-- ============================================================
SELECT
    t.ano,
    t.mes,
    t.nome_mes,
    COUNT(DISTINCT f.nk_pedido)                     AS total_pedidos,
    SUM(f.receita_liquida)                          AS receita_total,
    ROUND(SUM(f.receita_liquida)
        / NULLIF(COUNT(DISTINCT f.nk_pedido), 0), 2) AS ticket_medio
FROM fato_vendas f
JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
GROUP BY t.ano, t.mes, t.nome_mes
ORDER BY t.ano, t.mes;


-- ============================================================
-- KPI 3: Margem de Lucro Bruto por Categoria de Produto
-- Descrição: Percentual de margem bruta agrupado por categoria
-- ============================================================
SELECT
    p.categoria,
    p.subcategoria,
    SUM(f.receita_liquida)                          AS receita_total,
    SUM(f.custo_total)                              AS custo_total,
    SUM(f.lucro_bruto)                              AS lucro_bruto,
    ROUND(SUM(f.lucro_bruto)
        / NULLIF(SUM(f.receita_liquida), 0) * 100, 2) AS margem_pct
FROM fato_vendas f
JOIN dim_produto p ON f.sk_produto = p.sk_produto
WHERE p.registro_atual = 1                          -- BIT: TRUE -> 1
GROUP BY p.categoria, p.subcategoria
ORDER BY margem_pct DESC;


-- ============================================================
-- KPI 4: Ranking de Produtos Mais Vendidos (por Receita)
-- Descrição: Top 10 produtos por receita líquida
-- ============================================================
SELECT TOP 10                                       -- LIMIT -> TOP
    RANK() OVER (ORDER BY SUM(f.receita_liquida) DESC) AS ranking,
    p.nome_produto,
    p.categoria,
    p.subcategoria,
    SUM(f.quantidade)                               AS qtd_vendida,
    SUM(f.receita_liquida)                          AS receita_total,
    SUM(f.lucro_bruto)                              AS lucro_total
FROM fato_vendas f
JOIN dim_produto p ON f.sk_produto = p.sk_produto
WHERE p.registro_atual = 1                          -- BIT: TRUE -> 1
GROUP BY p.nome_produto, p.categoria, p.subcategoria
ORDER BY receita_total DESC;


-- ============================================================
-- KPI 5: Desempenho de Vendedores vs. Cota
-- Descrição: Comparativo entre receita realizada e cota anual
-- ============================================================
SELECT
    v.nome_completo                                 AS vendedor,
    v.territorio,
    t.ano,
    SUM(f.receita_liquida)                          AS receita_realizada,
    MAX(v.cota_anual)                               AS cota_anual,
    ROUND(SUM(f.receita_liquida)
        / NULLIF(MAX(v.cota_anual), 0) * 100, 2)   AS pct_atingimento_cota,
    CASE
        WHEN SUM(f.receita_liquida) >= MAX(v.cota_anual) THEN 'Atingiu'
        WHEN SUM(f.receita_liquida) >= MAX(v.cota_anual) * 0.8 THEN 'Próximo'
        ELSE 'Abaixo'
    END                                             AS status_cota
FROM fato_vendas f
JOIN dim_vendedor v ON f.sk_vendedor = v.sk_vendedor
JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
WHERE v.registro_atual = 1                          -- BIT: TRUE -> 1
GROUP BY v.nome_completo, v.territorio, t.ano
ORDER BY t.ano, pct_atingimento_cota DESC;


-- ============================================================
-- KPI 6: Receita por Território / Região
-- Descrição: Distribuição geográfica das vendas
-- ============================================================
SELECT
    ter.grupo                                       AS regiao_grupo,
    ter.pais,
    ter.nome_territorio,
    t.ano,
    COUNT(DISTINCT f.nk_pedido)                     AS total_pedidos,
    SUM(f.quantidade)                               AS qtd_itens,
    SUM(f.receita_liquida)                          AS receita_total,
    ROUND(SUM(f.receita_liquida)
        / SUM(SUM(f.receita_liquida)) OVER (PARTITION BY t.ano) * 100, 2) AS pct_receita_ano
FROM fato_vendas f
JOIN dim_territorio ter ON f.sk_territorio = ter.sk_territorio
JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
GROUP BY ter.grupo, ter.pais, ter.nome_territorio, t.ano
ORDER BY t.ano, receita_total DESC;


-- ============================================================
-- KPI 7: Taxa de Desconto e Impacto na Receita
-- Descrição: Análise do impacto das promoções na receita
-- ============================================================
SELECT
    pr.descricao                                    AS promocao,
    pr.tipo_desconto,
    pr.categoria,
    ROUND(AVG(pr.percentual_desconto) * 100, 2)    AS desconto_medio_pct,
    COUNT(DISTINCT f.nk_pedido)                     AS pedidos_com_desconto,
    SUM(f.quantidade)                               AS qtd_vendida,
    SUM(f.receita_bruta)                            AS receita_bruta_total,
    SUM(f.receita_liquida)                          AS receita_liquida_total,
    SUM(f.receita_bruta - f.receita_liquida)        AS total_desconto_concedido
FROM fato_vendas f
JOIN dim_promocao pr ON f.sk_promocao = pr.sk_promocao
GROUP BY pr.descricao, pr.tipo_desconto, pr.categoria, pr.percentual_desconto
ORDER BY total_desconto_concedido DESC;


-- ============================================================
-- KPI 8: Crescimento de Receita Ano a Ano (YoY)
-- Descrição: Comparativo de receita entre anos consecutivos
-- ============================================================
WITH receita_anual AS (
    SELECT
        t.ano,
        SUM(f.receita_liquida)  AS receita_total
    FROM fato_vendas f
    JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
    GROUP BY t.ano
)
SELECT
    ano,
    receita_total,
    LAG(receita_total) OVER (ORDER BY ano)          AS receita_ano_anterior,
    receita_total - LAG(receita_total) OVER (ORDER BY ano) AS variacao_absoluta,
    ROUND(
        (receita_total - LAG(receita_total) OVER (ORDER BY ano))
        / NULLIF(LAG(receita_total) OVER (ORDER BY ano), 0) * 100, 2
    )                                               AS crescimento_yoy_pct
FROM receita_anual
ORDER BY ano;


-- ============================================================
-- KPI 9: Frequência de Compra por Cliente (RFM Simplificado)
-- Descrição: Recência, Frequência e Valor Monetário dos clientes
-- ============================================================
WITH rfm AS (
    SELECT
        f.sk_cliente,
        MAX(t.data_completa)                        AS ultima_compra,
        COUNT(DISTINCT f.nk_pedido)                 AS frequencia,
        SUM(f.receita_liquida)                      AS valor_total,
        DATEDIFF(DAY, MAX(t.data_completa), CAST(GETDATE() AS DATE))
                                                    AS dias_desde_ultima_compra
        -- CURRENT_DATE -> CAST(GETDATE() AS DATE)
        -- subtração de datas -> DATEDIFF(DAY, ...)
    FROM fato_vendas f
    JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
    GROUP BY f.sk_cliente
)
SELECT
    c.nome_completo,
    c.tipo_cliente,
    c.pais,
    rfm.ultima_compra,
    rfm.dias_desde_ultima_compra                    AS recencia_dias,
    rfm.frequencia                                  AS total_pedidos,
    ROUND(rfm.valor_total, 2)                       AS valor_monetario,
    ROUND(rfm.valor_total / rfm.frequencia, 2)      AS ticket_medio,
    CASE
        WHEN rfm.dias_desde_ultima_compra <= 365
             AND rfm.frequencia >= 5
             AND rfm.valor_total >= 10000            THEN 'Premium'
        WHEN rfm.dias_desde_ultima_compra <= 730
             AND rfm.frequencia >= 2                 THEN 'Recorrente'
        ELSE 'Ocasional'
    END                                             AS segmento_cliente
FROM rfm
JOIN dim_cliente c ON rfm.sk_cliente = c.sk_cliente
WHERE c.registro_atual = 1                          -- BIT: TRUE -> 1
ORDER BY rfm.valor_total DESC;


-- ============================================================
-- KPI 10: Sazonalidade de Vendas por Trimestre
-- Descrição: Padrão sazonal das vendas ao longo dos trimestres
-- ============================================================
SELECT
    t.ano,
    t.trimestre,
    CONCAT('Q', t.trimestre, '/', t.ano)            AS periodo,
    COUNT(DISTINCT f.nk_pedido)                     AS total_pedidos,
    SUM(f.quantidade)                               AS qtd_itens,
    SUM(f.receita_liquida)                          AS receita_total,
    ROUND(AVG(f.receita_liquida), 2)                AS receita_media_por_item,
    ROUND(
        SUM(f.receita_liquida)
        / SUM(SUM(f.receita_liquida)) OVER (PARTITION BY t.ano) * 100, 2
    )                                               AS pct_receita_no_ano,
    ROUND(
        SUM(f.receita_liquida)
        / NULLIF(
            AVG(SUM(f.receita_liquida)) OVER (PARTITION BY t.trimestre), 0
        ) * 100, 2
    )                                               AS indice_sazonalidade
FROM fato_vendas f
JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
GROUP BY t.ano, t.trimestre
ORDER BY t.ano, t.trimestre;
