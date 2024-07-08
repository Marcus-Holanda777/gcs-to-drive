import duckdb
import os
from data_cod.secret import access_secret_version

DB_ID = os.environ['db_id']
DB_SECRET = os.environ['db_secret']
PROJECT_IC = os.environ['project_id']

KEY = access_secret_version(project_id=PROJECT_IC, secret_id=DB_ID, version_id=1)
SECRET = access_secret_version(project_id=PROJECT_IC, secret_id=DB_SECRET, version_id=1)

def etl_cadastro_uc(bucket, key):
    catalog = f'gc://{bucket}/{key}'

    key_to = f"{key.split('/')[0]}/cadastro.csv"
    catalog_to = f'gc://{bucket}/{key_to}'

    con = duckdb.connect(
        config={
            'threads': (os.cpu_count() or 1) * 5,
            'temp_directory': '/tmp/.tmp',
            'preserve_insertion_order': False
        }
    )

    # NOTE: Install LOAD credentials
    con.install_extension('httpfs')
    con.load_extension('httpfs')

    con.sql(f"""
        CREATE SECRET IF NOT EXISTS(
            TYPE gcs,
            KEY_ID {KEY},
            SECRET {SECRET}
    );
    """)

    con.sql(f"ATTACH {catalog} AS ultima_chance")
    con.sql("USE ultima_chance;")

    # TODO: Criar tabela de conversao dos meses
    con.sql("""
        CREATE OR REPLACE TABLE month_brazilian (
	        name VARCHAR,
	        nome VARCHAR
	 );""")
    
    # TODO: Inserir dados month_brazilian
    con.executemany(
        "INSERT INTO month_brazilian VALUES(?, ?)",
        [
            ('January','Janeiro'),
		    ('February','Fevereiro'),
		    ('March','Março'),
		    ('April','Abril'),
		    ('May','Maio'),
		    ('June','Junho'),
		    ('July','Julho'),
		    ('August','Agosto'),
		    ('September','Setembro'),
		    ('October','Outubro'),
		    ('November','Novembro'),
		    ('December','Dezembro')
        ]
    )

    # TODO: Criar macro de perdas
    con.sql("""
    CREATE OR REPLACE MACRO resumo_estoque(filtro := null)
    AS TABLE 
    (
        WITH movs_kardex
        as
        (
            PIVOT (
            FROM ultima_chance_kardex
            WHERE filtro IS NULL OR ulch_dh_movimentacao <= filtro::TIMESTAMP
            )
            ON ulch_fl_tipo_movimentacao in('S', 'E')
            USING sum(ulch_quantidade)	
            GROUP BY ulch_sq_produto
        ),
        movs_aut
        AS 
        ( 
            PIVOT (
                FROM ultima_chance_autorizacao
                WHERE filtro IS NULL 
                OR COALESCE(ulch_dh_venda, ulch_dh_cancelamento, xxxx_dh_alt, xxxx_dh_cad) <= filtro::TIMESTAMP
            )
            ON ulch_fl_situacao in('A', 'C', 'F', 'U', 'V')
            USING count(*)
            GROUP BY ulch_sq_produto
        ),
        categorias
        AS (
            SELECT 
                pm.prme_cd_produto       AS prme_cd_produto,
                pm.prme_tx_descricao1    AS prme_tx_descricao,
                n1.capn_ds_categoria     AS n01,
                n2.capn_ds_categoria     AS n02,
                n3.capn_ds_categoria     AS n03,
                uc.capn_tp_ultima_chance AS cat_uc,
                forn.forn_nm_fantasia    AS forn_nm
            FROM produto_mestre          AS pm
            INNER JOIN fornecedor AS forn using(forn_cd_fornecedor)
            LEFT JOIN categoria_produto_novo AS n1 ON SUBSTRING(pm.capn_cd_categoria, 1, 1) || '.000.000.00.00.00.00.00' = n1.capn_cd_categoria
            LEFT JOIN categoria_produto_novo AS n2 ON SUBSTRING(pm.capn_cd_categoria, 1, 5) || '.000.00.00.00.00.00' = n2.capn_cd_categoria
            LEFT JOIN categoria_produto_novo AS n3 ON SUBSTRING(pm.capn_cd_categoria, 1, 9) || '.00.00.00.00.00' = n3.capn_cd_categoria
            LEFT JOIN categoria_produto_novo AS uc ON pm.capn_cd_categoria = uc.capn_cd_categoria
        ),
        hierarquia
        AS (
            SELECT 
                fil.fili_cd_filial,
                COALESCE(gr.codinome, 'ND')  AS gerente_regional,
                COALESCE(gos.codinome, 'ND') AS gerente_operacional
                -- COALESCE(s.supervisor, 'ND') AS supervisor
            FROM filial AS fil
            LEFT JOIN assist_ger_regional AS agr using(asgr_cd_usuario)
            LEFT JOIN gerente_regional AS gr using(gere_cd_usuario)
            LEFT JOIN gerente_operacao AS gos using(geop_cd_usuario)
            -- LEFT JOIN supervisor s ON fil.fili_cd_filial = s.fili_cd_filial
        ),
        estoque
        AS 
        (
            SELECT 
                ucp.ulch_sq_produto,
                hr.gerente_regional,
                hr.gerente_operacional,
                -- hr.supervisor,
                ucp.fili_cd_filial                                AS filial,
                ucp.prme_cd_produto                               AS cod_prod,
                strip_accents(trim(categ.prme_tx_descricao))      AS nm_prod,
                strip_accents(trim(categ.n01))                    AS categ_n01,
                strip_accents(trim(categ.n02))                    AS categ_n02,
                strip_accents(trim(categ.n03))                    AS categ_n03,
                strip_accents(trim(categ.cat_uc))                 AS cat_uc,
                strip_accents(trim(categ.forn_nm))                AS forn_nm,
                strip_accents(trim(ulch_lote))                    AS lote,
                CONCAT(pf.prfi_tp_clabcfat, pf.prfi_tp_sclabcfat) AS class_vendas,
                ucp.ulch_quantidade                               AS qtd_cadastrada,
                pf.prfi_qt_estoqatual                             AS qtd_estoque_loja,
                pf.prfi_qt_estindisp                              AS qtd_estoque_indisponivel_loja,
                pf.prfi_vl_cmpcsicms                              AS preco_unit,
                concat(
                (SELECT nome FROM month_brazilian WHERE name = monthname(ulch_dt_vencimento))
                , ' de ', 
                YEAR(ulch_dt_vencimento)
                ) AS mes_ano_de_data_emissao,
                -- MEDICAMENTOS RECOLHIMENTO UM MES ANTES DE VENCER, CONSIDERAR ATE O DIA 18,
                -- DIA DO MES
                -- NAO MEDICAMENTOS, ATÉ O DIA 15 DO MES DA VALIDADE
                CASE
                    WHEN categ.n01[1] = 'M' THEN 
                    strftime(ucp.ulch_dt_vencimento - INTERVAL 1 MONTH, '%Y-%m-18')::timestamp
                    ELSE ucp.ulch_dt_vencimento + INTERVAL 14 DAYS	
                END::date AS recolher
            FROM ultima_chance_produto as ucp
            inner join categorias     as categ using(prme_cd_produto)
            inner join produto_filial as pf on ucp.fili_cd_filial = pf.fili_cd_filial and ucp.prme_cd_produto = pf.prme_cd_produto
            inner join hierarquia     AS hr on ucp.fili_cd_filial = hr.fili_cd_filial
            -- RECOLHIMENTO MAIOR OU IGUAL A DATA DE HOJE
            WHERE 
            (filtro IS NULL OR recolher >= filtro::date AND xxxx_dh_cad <= filtro::timestamp)
                AND
            (filtro IS NOT NULL or recolher >= CURRENT_DATE)
        ),
        saldo_estoque
        AS (
            SELECT 
                est.*,
                COALESCE(kd.E, 0)  AS E,
                COALESCE(kd.S, 0)  AS S,
                COALESCE(aut.A, 0) AS A,
                COALESCE(aut.C, 0) AS C,
                COALESCE(aut.F, 0) AS F,
                COALESCE(aut.U, 0) AS U,
                CASE WHEN filtro IS NULL THEN COALESCE(aut.V, 0) ELSE 0 END AS V,
                -- SALDO DE ESTOQUE, CADASTRO - (CANCELADOS + FINALIZADOS + EM_USO + VENCIDOS)
                CASE WHEN FILTRO IS NULL THEN
                    CASE WHEN (est.qtd_cadastrada - (C + F + U + V)) <= qtd_estoque_loja
                        THEN est.qtd_cadastrada - (C + F + U + V) 
                        ELSE qtd_estoque_loja 
                    END
                ELSE 
                    est.qtd_cadastrada - (C + F + U + V)
                END as saldo,
                saldo * preco_unit AS valor_saldo
            from estoque          AS est
            left join movs_kardex AS kd  ON est.ulch_sq_produto = kd.ulch_sq_produto
            left join movs_aut    AS aut ON est.ulch_sq_produto = aut.ulch_sq_produto
            WHERE
                saldo > 0
        )
        from saldo_estoque
    );
    """)

    # TODO: Exportar cadastro
    con.sql(f"""
        COPY (FROM resumo_estoque())
	    TO {catalog_to} (HEADER, DELIMITER ';');
    """
    )