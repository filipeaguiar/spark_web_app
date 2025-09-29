SELECT
    pdm.prontuario,
    pdm.codigo,
    pdm.nome,
    COALESCE('L: ' || pdm.lto_lto_id, 'Q: ' || qrt.descricao, 'U: ' || unf2.descricao) AS localizacao,
    pdm.dthr_inicio AS inicio_prescricao,
    pdm.dthr_fim AS fim_prescricao,
    unf.descricao AS unidade_dispensacao,
    ser.usuario AS triado_por,
    dsm.dthr_triado,
    ser2.usuario AS entregue_por,
    dsm.dthr_entrega,
    med.descricao AS medicamento
FROM
    agh.v_afa_prcr_disp_mdtos pdm
    LEFT JOIN agh.afa_dispensacao_mdtos dsm ON dsm.atd_seq = pdm.atd_seq
        AND dsm.pme_seq = pdm.seq
    LEFT JOIN agh.afa_medicamentos med ON dsm.med_mat_codigo = med.mat_codigo
    LEFT JOIN agh.rap_servidores ser ON dsm.ser_matricula_triado_por = ser.matricula
        AND dsm.ser_vin_codigo_triado_por = ser.vin_codigo
    LEFT JOIN agh.rap_servidores ser2 ON dsm.ser_matricula_entregue = ser2.matricula
        AND dsm.ser_vin_codigo_entregue = ser2.vin_codigo
    LEFT JOIN agh.agh_unidades_funcionais unf ON dsm.unf_seq = unf.seq
    LEFT JOIN agh.agh_unidades_funcionais unf2 ON dsm.unf_seq_solicitante = unf2.seq
    LEFT JOIN agh.ain_quartos qrt ON pdm.qrt_numero = qrt.numero
WHERE
    1 = 1
    -- AND pdm.dt_referencia::date = :data_inicial
    --  AND pdm.prontuario = :prontuario
    AND ((pdm.atd_seq,
            pdm.seq) IN (
            SELECT
                pme_atd_seq,
                pme_seq
            FROM
                agh.afa_dispensacao_mdtos dsm
            WHERE
                1 = 1
                AND dsm.atd_seq = pdm.atd_seq
                AND dsm.pme_seq = pdm.seq
                AND dsm.ind_situacao IN ('S', 'D', 'T', 'C')
                --          AND dsm.unf_seq = :codigo_farmacia
)
            OR (pdm.atd_seq,
                pdm.seq) IN (
                SELECT
                    pme_atd_seq,
                    pme_seq
                FROM
                    agh.afa_dispensacao_mdtos dsm
                WHERE
                    1 = 1
                    AND dsm.imo_pmo_pte_atd_seq = pdm.atd_seq
                    AND dsm.imo_pmo_pte_seq = pdm.seq
                    AND dsm.ind_situacao IN ('S', 'D', 'T', 'C')
                    --   AND dsm.unf_seq = :codigo_farmacia
))
        AND pdm.atd_seq_local IN (
            SELECT
                atd.seq
            FROM
                agh.agh_atendimentos atd
            WHERE
                1 = 1
                AND atd.seq = pdm.atd_seq_local
                --      AND atd.unf_seq = :codigo_unidade_funcional
                --      AND atd.prontuario = :prontuario
                --      AND atd.lto_lto_id = :leito
                --      AND atd.qrt_numero = :numero_quarto
)
        AND dsm.ind_situacao IN ('S', 'D', 'T', 'C');

