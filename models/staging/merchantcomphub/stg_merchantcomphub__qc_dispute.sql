WITH column_transforms_and_renames AS (
	SELECT
	-- ids/tokens/keys
		qc_dispute.id AS dispute_id,
		qc_dispute.dispute_agent_id AS dispute_agent_registry_id,
		qc_dispute.notary_case_id,
		qc_dispute.target_id,
		qc_dispute.target_qc_decision_id,

	-- dates/timestamps
		qc_dispute.created_at AS dispute_created_at,
		qc_dispute.updated_at AS dispute_updated_at,

	-- booleans

	-- measures

	-- strings/varchars
		qc_dispute.additional_links,
		qc_dispute.dispute_comment,
		qc_dispute.dispute_agent_alias,
		qc_dispute.dispute_agent_name,
		qc_dispute.dispute_agent_team,
		qc_dispute.feedback_answers AS feedback_answers_json,
		qc_dispute.target_id_type,
		qc_dispute.triage_agent_alias,
		qc_dispute.triage_comment,
		qc_dispute.triage_decision,

	-- metadata
		qc_dispute.__cdc_create_timestamp,
		qc_dispute.__cdc_update_timestamp,
		qc_dispute.__db_name,
		qc_dispute.__deleted_upstream,
		qc_dispute.__entity_schema_name
		
	FROM
		merchantcomphub.raw_oltp.qc_dispute
	WHERE
		-- filters out QA test and sample data pre-launch date of Aug 1, 2024
		dispute_created_at::date >= '2024-08-01'
),
final AS (
	SELECT	
		dispute_id,
		dispute_agent_registry_id,
		notary_case_id,
		target_id,
		target_qc_decision_id,
		dispute_created_at,
		dispute_updated_at,
		additional_links,
		dispute_comment,
		dispute_agent_alias,
		dispute_agent_name,
		dispute_agent_team,
		feedback_answers_json,
		target_id_type,
		triage_agent_alias,
		triage_comment,
		triage_decision,
	FROM 
		column_transforms_and_renames
)
SELECT * FROM final
;