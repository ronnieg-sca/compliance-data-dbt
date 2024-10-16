WITH qc_decisions_slowly_changing_dimension AS (
	SELECT
		notary_case_id,
		assignment_id,
		decision_id,
		qc_agent_registry_id,
		qc_agent_name,
		qc_agent_alias,
		qc_team,
		ops_agent_registry_id,
		ops_agent_name,
		ops_agent_team,
		target_id,
		target_id_type,
		unit_token,
		ops_queue,
		country_code,	
		ops_agent_action,
		ops_agent_action_at,
		is_agent_follow_up_required,
		qc_score,
		qc_comment,
		qc_note,
		is_procedure_review,
		procedure_review_comment,
		product,
		smr_typology,
		sar_typology,
		sfs_account_type,
		target_qc_dispute_id,
		amlrs_agent_location,	
		is_sample_testing_or_training,
		qc_queue,
		qc_case_queue_category,
		decision_created_at AS effective_start_at,
		LEAD(decision_created_at) OVER (PARTITION BY decision_id ORDER BY decision_created_at) AS effective_end_at
	FROM
		app_compliance.sca_staging.merchantcomphub__qc_decision
	ORDER BY
		notary_case_id, 
		decision_created_at
),
business_current_flag AS (
	SELECT
		notary_case_id,
		assignment_id,
		decision_id,
		qc_agent_registry_id,
		qc_agent_name,
		qc_team,
		ops_agent_registry_id,
		ops_agent_name,
		ops_agent_team,
		target_id,
		target_id_type,
		unit_token,
		ops_queue,
		country_code,	
		ops_agent_action,
		ops_agent_action_at,
		is_agent_follow_up_required,
		qc_score,
		qc_comment,
		qc_note,
		is_procedure_review,
		procedure_review_comment,
		product,
		smr_typology,
		sar_typology,
		sfs_account_type,
		target_qc_dispute_id,
		amlrs_agent_location,	
		qc_agent_alias,
		is_sample_testing_or_training,
		qc_queue,
		qc_case_queue_category,
		effective_start_at,
		effective_end_at,
		CASE WHEN effective_end_at IS NULL THEN TRUE ELSE FALSE END AS is_current_decision
	FROM
		qc_decisions_slowly_changing_dimension
),
final AS (
	SELECT
		notary_case_id,
		assignment_id,
		decision_id,
		qc_agent_registry_id,
		qc_agent_name,
		qc_agent_alias,
		qc_team,
		ops_agent_registry_id,
		ops_agent_name,
		ops_agent_team,
		target_id,
		target_id_type,
		unit_token,
		ops_queue,
		country_code,	
		ops_agent_action,
		ops_agent_action_at,
		is_agent_follow_up_required,
		qc_score,
		qc_comment,
		qc_note,
		is_procedure_review,
		procedure_review_comment,
		product,
		smr_typology,
		sar_typology,
		sfs_account_type,
		target_qc_dispute_id,
		amlrs_agent_location,	
		is_sample_testing_or_training,
		qc_queue,
		qc_case_queue_category,
		effective_start_at,
		effective_end_at,
		is_current_decision
	FROM
		business_current_flag
)
SELECT * FROM final
;