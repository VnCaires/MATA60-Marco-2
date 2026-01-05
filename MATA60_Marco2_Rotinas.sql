/* ============================================================
   MATA60 – Banco de Dados
   Marco 2 – Rotinas Avançadas (VERSÃO COMPATÍVEL)
   Autor: Vinicius Oliveira Caires
   SGBD: PostgreSQL
   Pré-condição:
   - Script 1 (Marco 1 + Marco 2 unificado) já executado
   ============================================================ */


/* ============================================================
   TELA 1 – CADASTRO DE INSCRIÇÃO COM VALIDAÇÃO
   ============================================================ */

DROP MATERIALIZED VIEW IF EXISTS mv_inscricao_evento_status;

CREATE MATERIALIZED VIEW mv_inscricao_evento_status AS
SELECT
    id_evento,
    id_participante,
    st_status,
    COUNT(*) AS qt_inscricoes
FROM tb_inscricao
GROUP BY
    id_evento,
    id_participante,
    st_status;


/* Stored Procedure – versão compatível e segura */
CREATE OR REPLACE PROCEDURE sp_cadastro_inscricao(
    p_id_participante INTEGER,
    p_id_evento INTEGER,
    p_categoria TEXT,
    p_status TEXT
)
LANGUAGE SQL
AS $$
    INSERT INTO tb_inscricao (id_participante, id_evento, tp_categoria, st_status)
    SELECT p_id_participante, p_id_evento, p_categoria, 'PENDENTE'
    WHERE NOT EXISTS (
        SELECT 1
        FROM tb_inscricao
        WHERE id_participante = p_id_participante
          AND id_evento = p_id_evento
    );

    UPDATE tb_inscricao
    SET tp_categoria = p_categoria,
        st_status   = p_status
    WHERE id_participante = p_id_participante
      AND id_evento = p_id_evento;
$$;


/* ============================================================
   TELA 2 – MANUTENÇÃO DE ARTIGOS CIENTÍFICOS
   ============================================================ */

DROP MATERIALIZED VIEW IF EXISTS mv_artigos_evento_trilha;

CREATE MATERIALIZED VIEW mv_artigos_evento_trilha AS
SELECT
    id_evento,
    no_trilha,
    COUNT(*) AS qt_artigos
FROM tb_artigo
GROUP BY
    id_evento,
    no_trilha;


/* Stored Procedure – alinhada ao Script 1 */
CREATE OR REPLACE PROCEDURE sp_manter_artigo(
    p_id_artigo INTEGER,
    p_titulo TEXT,
    p_trilha TEXT,
    p_id_evento INTEGER
)
LANGUAGE SQL
AS $$
    INSERT INTO tb_artigo (id_artigo, no_titulo, no_trilha, id_evento)
    SELECT p_id_artigo, p_titulo, p_trilha, p_id_evento
    WHERE NOT EXISTS (
        SELECT 1 FROM tb_artigo WHERE id_artigo = p_id_artigo
    );

    UPDATE tb_artigo
    SET no_titulo = p_titulo,
        no_trilha = p_trilha
    WHERE id_artigo = p_id_artigo;
$$;


/* ============================================================
   DASHBOARD ANALÍTICO ESTRATÉGICO
   ============================================================ */

DROP MATERIALIZED VIEW IF EXISTS mv_evento_financeiro;

CREATE MATERIALIZED VIEW mv_evento_financeiro AS
SELECT
    e.id_evento,
    e.no_evento,
    COUNT(i.id_inscricao) AS qt_inscricoes,
    COALESCE(SUM(p.vl_pagamento), 0) AS vl_total_arrecadado
FROM tb_evento e
LEFT JOIN tb_inscricao i
    ON i.id_evento = e.id_evento
LEFT JOIN tb_pagamento p
    ON p.id_inscricao = i.id_inscricao
GROUP BY
    e.id_evento,
    e.no_evento;


DROP MATERIALIZED VIEW IF EXISTS mv_desempenho_trilha;

CREATE MATERIALIZED VIEW mv_desempenho_trilha AS
SELECT
    a.no_trilha,
    COUNT(r.id_revisao) AS qt_revisoes,
    AVG(r.nu_nota)::NUMERIC(4,2) AS media_notas
FROM tb_artigo a
JOIN tb_revisao r
    ON r.id_artigo = a.id_artigo
GROUP BY
    a.no_trilha;


/* Stored Procedure – estratégica */
CREATE OR REPLACE PROCEDURE sp_refresh_dashboard_estrategico()
LANGUAGE SQL
AS $$
    REFRESH MATERIALIZED VIEW mv_evento_financeiro;
    REFRESH MATERIALIZED VIEW mv_desempenho_trilha;
$$;


/* ============================================================
   DASHBOARD ANALÍTICO OPERACIONAL
   ============================================================ */

DROP MATERIALIZED VIEW IF EXISTS mv_dashboard_operacional_intermediario;

CREATE MATERIALIZED VIEW mv_dashboard_operacional_intermediario AS
SELECT
    e.id_evento,
    e.no_evento,
    i.st_status,
    COUNT(i.id_inscricao) AS qt_inscricoes
FROM tb_evento e
JOIN tb_inscricao i
    ON i.id_evento = e.id_evento
JOIN tb_participante p
    ON p.id_participante = i.id_participante
GROUP BY
    e.id_evento,
    e.no_evento,
    i.st_status;


DROP MATERIALIZED VIEW IF EXISTS mv_dashboard_operacional_avancado;

CREATE MATERIALIZED VIEW mv_dashboard_operacional_avancado AS
SELECT
    e.id_evento,
    e.no_evento,
    COUNT(DISTINCT a.id_artigo) AS qt_artigos,
    (
        SELECT COUNT(*)
        FROM tb_revisao r
        JOIN tb_artigo a2
            ON a2.id_artigo = r.id_artigo
        WHERE a2.id_evento = e.id_evento
    ) AS qt_revisoes
FROM tb_evento e
JOIN tb_artigo a
    ON a.id_evento = e.id_evento
WHERE EXISTS (
    SELECT 1
    FROM tb_sessao s
    WHERE s.id_evento = e.id_evento
)
GROUP BY
    e.id_evento,
    e.no_evento;


/* Stored Procedure – operacional */
CREATE OR REPLACE PROCEDURE sp_refresh_dashboard_operacional()
LANGUAGE SQL
AS $$
    REFRESH MATERIALIZED VIEW mv_dashboard_operacional_intermediario;
    REFRESH MATERIALIZED VIEW mv_dashboard_operacional_avancado;
$$;
