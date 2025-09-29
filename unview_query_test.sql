-- SQL Gerado automaticamente pelo script unview.py --

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
    (
-- START VIEW: agh.v_afa_prcr_disp_mdtos
SELECT vpcr.atd_seq,
    vpcr.seq,
    vpcr.dt_referencia,
    vpcr.dthr_inicio,
    vpcr.dthr_fim,
    vpcr.atd_seq_local,
    atendimento.prontuario,
    atendimento.lto_lto_id,
    atendimento.qrt_numero,
    atendimento.unf_seq,
    atendimento.trp_seq,
    paciente.codigo,
    paciente.nome,
    sumarios.apa_atd_seq,
    sumarios.apa_seq,
    sumarios.seqp,
    count(dispmdtossolic.seq) AS countsolic,
    count(dispmdtosdisp.seq) AS countdisp,
    count(dispmdtosconf.seq) AS countconf,
    count(dispmdtosenv.seq) AS countenv,
    count(dispmdtostriado.seq) AS counttriado,
    count(dispmdtosocorr.seq) AS countocorr,
    clinicas.codigo AS clinica_codigo
   FROM (((((((((((((
-- START VIEW: agh.v_afa_prcr_disp
SELECT pme.atd_seq,
    pme.seq,
    pme.dt_referencia,
    pme.dthr_inicio,
    pme.dthr_fim,
    pme.atd_seq AS atd_seq_local
   FROM agh.mpm_prescricao_medicas pme
UNION
 SELECT pte.atd_seq,
    pte.seq,
    pte.dt_prev_execucao AS dt_referencia,
    pte.dt_prev_execucao AS dthr_inicio,
    pte.dt_prev_execucao AS dthr_fim,
    COALESCE(( SELECT atd_1.seq
           FROM agh.agh_atendimentos atd_1
          WHERE (((atd_1.pac_codigo = ( SELECT atd1.pac_codigo
                   FROM agh.agh_atendimentos atd1
                  WHERE (atd1.seq = pte.atd_seq))) AND (substr((atd_1.origem)::text, 1, 1) = ANY (ARRAY['I'::text, 'U'::text]))) AND (substr((atd_1.ind_pac_atendimento)::text, 1, 1) = 'S'::text))), pte.atd_seq) AS atd_seq_local
   FROM agh.agh_atendimentos atd,
    agh.mpt_prescricao_pacientes pte
  WHERE ((atd.seq = pte.atd_seq) AND (atd.ind_tipo_tratamento = (27)::smallint))
-- END VIEW: agh.v_afa_prcr_disp
) vpcr
     LEFT JOIN agh.agh_atendimentos atendimento ON ((vpcr.atd_seq = atendimento.seq)))
     LEFT JOIN agh.agh_unidades_funcionais auf ON ((atendimento.unf_seq = auf.seq)))
     LEFT JOIN agh.agh_clinicas clinicas ON ((auf.clc_codigo = clinicas.codigo)))
     LEFT JOIN agh.aip_pacientes paciente ON ((atendimento.pac_codigo = paciente.codigo)))
     LEFT JOIN agh.mpm_alta_sumarios sumarios ON (((vpcr.atd_seq = sumarios.apa_atd_seq) AND ((sumarios.ind_concluido)::text = 'S'::text))))
     LEFT JOIN agh.mpm_prescricao_medicas prescricao ON (((prescricao.atd_seq = vpcr.atd_seq) AND (prescricao.seq = vpcr.seq))))
     LEFT JOIN agh.afa_dispensacao_mdtos dispmdtossolic ON (((((dispmdtossolic.pme_atd_seq = prescricao.atd_seq) AND (dispmdtossolic.pme_seq = prescricao.seq)) OR ((dispmdtossolic.imo_pmo_pte_atd_seq = prescricao.atd_seq) AND (dispmdtossolic.imo_pmo_pte_seq = prescricao.seq))) AND ((dispmdtossolic.ind_situacao)::text = 'S'::text))))
     LEFT JOIN agh.afa_dispensacao_mdtos dispmdtosdisp ON (((((dispmdtosdisp.pme_atd_seq = prescricao.atd_seq) AND (dispmdtosdisp.pme_seq = prescricao.seq)) OR ((dispmdtosdisp.imo_pmo_pte_atd_seq = prescricao.atd_seq) AND (dispmdtosdisp.imo_pmo_pte_seq = prescricao.seq))) AND ((dispmdtosdisp.ind_situacao)::text = 'D'::text))))
     LEFT JOIN agh.afa_dispensacao_mdtos dispmdtosconf ON (((((dispmdtosconf.pme_atd_seq = prescricao.atd_seq) AND (dispmdtosconf.pme_seq = prescricao.seq)) OR ((dispmdtosconf.imo_pmo_pte_atd_seq = prescricao.atd_seq) AND (dispmdtosconf.imo_pmo_pte_seq = prescricao.seq))) AND ((dispmdtosconf.ind_situacao)::text = 'C'::text))))
     LEFT JOIN agh.afa_dispensacao_mdtos dispmdtosenv ON (((((dispmdtosenv.pme_atd_seq = prescricao.atd_seq) AND (dispmdtosenv.pme_seq = prescricao.seq)) OR ((dispmdtosenv.imo_pmo_pte_atd_seq = prescricao.atd_seq) AND (dispmdtosenv.imo_pmo_pte_seq = prescricao.seq))) AND ((dispmdtosenv.ind_situacao)::text = 'E'::text))))
     LEFT JOIN agh.afa_dispensacao_mdtos dispmdtostriado ON (((((((((dispmdtostriado.pme_atd_seq = prescricao.atd_seq) AND (dispmdtostriado.pme_seq = prescricao.seq)) OR ((dispmdtostriado.imo_pmo_pte_atd_seq = prescricao.atd_seq) AND (dispmdtostriado.imo_pmo_pte_seq = prescricao.seq))) AND ((dispmdtostriado.ind_situacao)::text = 'T'::text)) AND (dispmdtostriado.qtde_dispensada IS NOT NULL)) AND (dispmdtostriado.qtde_dispensada > (0)::double precision)) AND (dispmdtostriado.qtde_estornada IS NULL)) AND (dispmdtostriado.tod_seq IS NULL))))
     LEFT JOIN agh.afa_dispensacao_mdtos dispmdtosocorr ON ((((((dispmdtosocorr.pme_atd_seq = prescricao.atd_seq) AND (dispmdtosocorr.pme_seq = prescricao.seq)) OR ((dispmdtosocorr.imo_pmo_pte_atd_seq = prescricao.atd_seq) AND (dispmdtosocorr.imo_pmo_pte_seq = prescricao.seq))) AND ((dispmdtosocorr.ind_situacao)::text = 'T'::text)) AND (((dispmdtosocorr.qtde_estornada IS NOT NULL) AND (dispmdtosocorr.qtde_estornada > (0)::double precision)) OR (dispmdtosocorr.tod_seq IS NOT NULL)))))
  GROUP BY vpcr.atd_seq, vpcr.seq, vpcr.dt_referencia, vpcr.dthr_inicio, vpcr.dthr_fim, vpcr.atd_seq_local, atendimento.prontuario, atendimento.lto_lto_id, atendimento.qrt_numero, atendimento.unf_seq, atendimento.trp_seq, sumarios.apa_atd_seq, sumarios.apa_seq, sumarios.seqp, clinicas.codigo, paciente.codigo, paciente.nome
-- END VIEW: agh.v_afa_prcr_disp_mdtos
) pdm
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
                    
))
        AND pdm.atd_seq_local IN (
            SELECT
                atd.seq
            FROM
                agh.agh_atendimentos atd
            WHERE
                1 = 1
                AND atd.seq = pdm.atd_seq_local
                
                
                
                
)
        AND dsm.ind_situacao IN ('S', 'D', 'T', 'C');