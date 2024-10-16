WITH decisions_business_current_records AS (
	SELECT * FROM app_compliance.sca_transform.square_quality_assurance_case_decision_scd WHERE is_current_decision
),
disputes AS (
	SELECT * FROM app_compliance.sca_staging.merchantcomphub__qc_dispute
),
decisions_keyword AS (
	SELECT * FROM app_compliance.sca_staging.merchantcomphub__qc_question_answer
),
ops_agent_follow_up_complete AS (
	SELECT * FROM app_compliance.sca_staging.merchantcomphub__qc_decision_followup
),
square_regulator_cases AS (
	SELECT * FROM app_compliance.seller.pos_cases
),
compliance_operations_employees AS (
	SELECT * FROM app_compliance.sca_transform.quality_assurance_operations_agents_scored
),
quality_assurance_employees AS (
	SELECT * FROM app_compliance.cash.workday_compliance_employees
),
qc_decision_case_details AS (
	SELECT
		qc_decision.decision_id
		, qc_decision.effective_start_at AS decision_created_at
		, qc_decision.qc_agent_registry_id
		, qc_decision.qc_agent_alias
		, qa_employees.employee_name AS qc_agent_name
		, qc_decision.notary_case_id
		, qc_decision.assignment_id
		, qc_decision.unit_token
		, qc_decision.ops_agent_action
		, qc_decision.ops_agent_action_at
		, qc_decision.qc_score
		, LISTAGG(DISTINCT qc_keyword.keyword, ', ') AS keyword
		, qc_decision.qc_comment
		, qc_decision.is_agent_follow_up_required
		, CASE
			WHEN follow_up.follow_up_action_id IS NOT NULL AND qc_decision.is_agent_follow_up_required = TRUE
			THEN TRUE ELSE FALSE
		END AS has_agent_completed_follow_up
		, qc_decision.is_sample_testing_or_training
		, qc_decision.procedure_review_comment
		, qc_decision.country_code
		, qc_decision.product
		, qc_decision.ops_agent_registry_id
		, qc_decision.ops_agent_name
		, qc_decision.ops_agent_team
		, ops_employees.ops_agent_employee_id
		, ops_employees.workday_email AS ops_agent_workday_email
		, ops_employees.alternate_email AS ops_agent_alternate_email
		, ops_employees.current_direct_lead_employee_id AS ops_agent_current_direct_lead_employee_id
		, ops_employees.current_direct_lead AS ops_agent_current_direct_lead
		, ops_employees.country AS ops_agent_country
		, ops_employees.headcount_category
		, ops_employees.current_direct_lead_workday_email AS ops_agent_direct_leads_email
		, ops_employees.current_direct_lead_alternate_email AS ops_agent_direct_leads_alternate_email
		, qc_decision.qc_queue
		, qc_decision.qc_case_queue_category
		, qc_decision.ops_queue
		, qc_decision.amlrs_agent_location
		, qc_dispute.dispute_id
		, qc_dispute.dispute_created_at
		, qc_dispute.dispute_agent_name
		, qc_dispute.dispute_agent_team
		, qc_dispute.dispute_comment
		, qc_dispute.triage_decision
		, qc_dispute.triage_comment
		, qc_dispute.triage_agent_alias
		, COALESCE(regulator.case_token, qc_decision.target_id) as target_id
		, regulator.originating_queue
		, regulator.originating_queue_business_queue_category
		, regulator.business_queue_category
	FROM
		decisions_business_current_records AS qc_decision
	LEFT JOIN
		disputes AS qc_dispute ON qc_decision.decision_id = qc_dispute.target_qc_decision_id
	LEFT JOIN
		decisions_keyword AS qc_keyword ON qc_decision.decision_id = qc_keyword.decision_id
	LEFT JOIN
		ops_agent_follow_up_complete AS follow_up ON qc_decision.decision_id = follow_up.decision_id
	LEFT JOIN
		square_regulator_cases AS regulator ON qc_decision.target_id = regulator.case_token
	LEFT JOIN
		compliance_operations_employees AS ops_employees ON qc_decision.ops_agent_registry_id = ops_employees.registry_id
	LEFT JOIN
		quality_assurance_employees AS qa_employees ON qc_decision.qc_agent_registry_id = qa_employees.uid
	GROUP BY ALL
),
final AS (
SELECT DISTINCT
		decision_id
		, decision_created_at
		, qc_agent_registry_id
		, qc_agent_alias
		, qc_agent_name
		, notary_case_id
		, assignment_id
		, unit_token
		, ops_agent_action
		, ops_agent_action_at
		, qc_score
		, keyword
		, qc_comment
		, is_agent_follow_up_required
		, has_agent_completed_follow_up
		, is_sample_testing_or_training
		, procedure_review_comment
		, country_code
		, product
		, ops_agent_registry_id
		, ops_agent_name
		, ops_agent_team
		, ops_agent_employee_id
		, ops_agent_workday_email
		, ops_agent_alternate_email
		, ops_agent_current_direct_lead_employee_id
		, ops_agent_current_direct_lead
		, headcount_category
		, ops_agent_country 
		, ops_agent_direct_leads_email
		, ops_agent_direct_leads_alternate_email
		, qc_queue
		, qc_case_queue_category
		, ops_queue
		, amlrs_agent_location
		, dispute_id
		, dispute_created_at
		, dispute_agent_name
		, dispute_agent_team
		, dispute_comment
		, triage_decision
		, triage_comment
		, triage_agent_alias
		, target_id
		, originating_queue
		, originating_queue_business_queue_category
		, business_queue_category
FROM
	qc_decision_case_details
)
SELECT * FROM final
;