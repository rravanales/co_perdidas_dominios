-- File: data_processing/transformation.hql
--
-- Description:
-- This HiveQL script performs the transformation of raw data from the staging tables
-- into a consolidated final table named "dominio_cliente" in the "co_perdidas" database.
-- The script applies data cleaning operations (e.g., TRIM on string fields) and type casting
-- where necessary. It also maps and renames fields as per the provided critical columns mapping,
-- and joins the tables based on the primary/foreign key relationships.
--
-- Inputs:
--   - Staging tables in the co_opera database: COM_ACT_ECONOMICA, COM_CATEGORIA, CRT_CUENTA,
--     NUC_CLIENTE, NUC_CUENTA, NUC_PERSONA, NUC_SERVICIO, NUC_TIP_DOC_PERSONA.
--
-- Outputs:
--   - Final consolidated Hive table: dominio_cliente.
--
-- Notes:
--   - Review and adjust join conditions if actual foreign key relationships differ.
--   - Ensure that the staging tables are created and populated by executing the hive_table_creation.hql script.
--   - The script assumes that string cleaning (trimming) is required on key fields.
--   - All necessary type conversions should already be handled in the staging process; additional
--     CAST operations can be added if needed.
--

-- Create final consolidated table dominio_cliente in the co_perdidas database
CREATE TABLE IF NOT EXISTS co_perdidas.dominio_cliente
STORED AS PARQUET
AS
SELECT
    -- From NUC_SERVICIO table
    ns.estado,
    ns.fec_finalizacion,
    ns.fec_inicio,
    ns.fecha_ultima_val,
    ns.id_cuenta,
    ns.id_servicio,
    ns.id_ult_agrup_ciclo,
    ns.nro_servicio,
    ns.tipo_servicio,
    -- From NUC_CUENTA table
    nc.id_cliente,
    nc.nro_cuenta,
    -- From CRT_CUENTA table (with trimming on string fields)
    ct.cod_categoria,
    ct.estrato,
    ct.id_persona,
    ct.id_state,
    ct.id_ult_agrup_ciclo AS id_ult_agrup_ciclo_crct,
    ct.nivel_tension,
    TRIM(ct.nombre) AS nombre_crct,
    ct.ruta_fact,
    ct.sucursal,
    -- From COM_ACT_ECONOMICA table
    ae.des_act_economica,
    ae.id_act_economica,
    -- From COM_CATEGORIA table (renamed column)
    cc.id_categoria AS id_categoria_com,
    -- From NUC_PERSONA table (with trimming on name)
    np.apellido_mat,
    np.apellido_pat,
    np.id_tip_doc_persona,
    TRIM(np.nombre) AS nombre_pers,
    np.nro_docto_ident,
    -- From NUC_TIP_DOC_PERSONA table
    tdp.des_tip_doc_persona
FROM co_opera.NUC_SERVICIO ns
JOIN co_opera.NUC_CUENTA nc
  ON ns.id_cuenta = nc.id_cuenta
JOIN co_opera.CRT_CUENTA ct
  ON nc.id_cliente = ct.id_persona
JOIN co_opera.COM_ACT_ECONOMICA ae
  ON ct.id_cuenta = ae.ID_ACT_ECONOMICA
JOIN co_opera.COM_CATEGORIA cc
  ON ae.ID_CATEGORIA = cc.ID_CATEGORIA
JOIN co_opera.NUC_PERSONA np
  ON ct.id_persona = np.ID_PERSONA
JOIN co_opera.NUC_TIP_DOC_PERSONA tdp
  ON np.ID_TIP_DOC_PERSONA = tdp.ID_TIP_DOC_PERSONA;