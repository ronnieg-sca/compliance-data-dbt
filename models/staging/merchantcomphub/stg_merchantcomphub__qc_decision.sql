WITH column_transforms_and_renames AS (
	SELECT
	-- ids/tokens/keys
		id AS decision_id,
		assignment_id,
		notary_case_id,
		ops_agent_id AS ops_agent_registry_id,
		qc_agent_id AS qc_agent_registry_id,
		target_id,
		target_qc_dispute_id,
		unit_token,

	-- dates/timestamps
		ops_agent_action_time AS ops_agent_action_at, -- AS actioned_time in current app_comp_qc.view LookML,
		created_at AS decision_created_at,
		updated_at AS decision_updated_at,

	-- booleans
		follow_up_required AS is_agent_follow_up_required,
		procedure_review AS is_procedure_review,
		sample_testing_or_training AS is_sample_testing_or_training,

	-- measures
		qc_score,

	-- strings/varchars
		amlrs_agent_location,
		country_code,	
		ops_agent_action, -- AS agent_action in current app_comp_qc.view LookML,
		ops_agent_name,
		ops_agent_team, -- AS agent_team in current app_comp_qc.view LookML,
		qc_comment, -- AS comment in current app_comp_qc.view LookML,
		qc_agent_alias,
		qc_agent_name,
		qc_note,
		qc_team,
		queue_slug as qc_queue,
		CASE WHEN qc_queue ILIKE 'square_amlrs%' THEN TRUE ELSE FALSE END AS is_amlrs,
		REPLACE(qc_queue, 'square_quality_control_') AS qc_case_queue_category,
		CASE
			WHEN product ILIKE '%payroll%' THEN 'Payroll'
			WHEN product ILIKE '%cash%' THEN 'Cash'
			WHEN product ILIKE '%BTC%' THEN 'Cash'
			WHEN REGEXP_LIKE(unit_token, '(C_).*') THEN 'Cash'
			WHEN product ILIKE '%capital%' THEN 'Capital'
			WHEN product ILIKE '%seller%' THEN 'POS'
		END AS product,
		smr_typology,
		sar_typology,
		sfs_account_type,
		target_id_type,
		target_queue_slug AS ops_queue,
		procedure_review_comment,	

	-- metadata
		__cdc_create_timestamp,
		__cdc_update_timestamp,
		__db_name,
		__entity_schema_name,
		__deleted_upstream

	FROM 
		{{source('merchantcomphub','qc_decision')}}
	WHERE
		-- filters out non-AMLRS QA test and sample data pre-launch date of Aug 1, 2024
		(decision_created_at::date >= '2024-08-01')
	OR
		-- filters out AMLRS QA test and sample data pre-launch date of Sep 1, 2024
		(qc_team = 'AMLRS QC' AND decision_created_at::date >= '2024-09-01')

),
final AS (
	SELECT
		decision_id,
		assignment_id,
		notary_case_id,
		ops_agent_registry_id,
		qc_agent_registry_id,
		target_id,
		target_qc_dispute_id,
		unit_token,
		ops_agent_action_at,
		decision_created_at,
		decision_updated_at,
		is_agent_follow_up_required,
		is_procedure_review,
		is_sample_testing_or_training,
		qc_score,
		amlrs_agent_location,
		country_code,	
		ops_agent_action,
		ops_agent_name,
		ops_agent_team,
		qc_comment,
		qc_agent_alias,
		qc_agent_name,
		qc_team,
		qc_note,
		qc_queue,
		qc_case_queue_category,
		product,
		smr_typology,
		sar_typology,
		sfs_account_type,
		target_id_type,
		ops_queue,
		procedure_review_comment,
	FROM
		column_transforms_and_renames
)
SELECT * FROM final