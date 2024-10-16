WITH operation_agents_with_cases_scored AS (
	SELECT DISTINCT
		ops_agent_registry_id AS registry_id
		, ops_agent_name
	FROM
		app_compliance.sca_transform.square_quality_assurance_case_decision_scd
	WHERE
		is_current_decision
),
square_compliance_quality_assurance_operation_agents AS (
	SELECT
		workday.employee_id AS ops_agent_employee_id
		, ops_agents.registry_id 
		, workday.first_name || ' ' || last_name AS employee_name
		, workday.email AS workday_email
		, CASE 
			WHEN workday_email LIKE '%@squareup.com' THEN REPLACE(workday_email, '@squareup.com', '@block.xyz')
			WHEN workday_email LIKE '%@block.xyz' THEN REPLACE(workday_email, '@block.xyz', '@squareup.com')
			ELSE workday_email
		END AS alternate_email
		, workday.active_status
		, workday.city
		, workday.state
		, workday.country
		, CASE 
			WHEN workday.office IN ('IN - Remote') THEN 'AMLRS India' 
			WHEN workday.office IN ('BG - Sofia - Remote') THEN 'AMLRS Bulgaria' 
			WHEN workday.worker_type = 'Contingent Worker' AND workday.country IN ('United States of America', 'Canada') THEN 'AMLRS North America'
			WHEN workday.worker_type = 'Contingent Worker' AND workday.country = 'Japan' THEN 'Japan Contractor' 
			WHEN workday.worker_type = 'Employee' THEN 'FTE Internal' 
			ELSE workday.office 
		END AS location
		, workday.hire_date
		, workday.termination_date
		, workday.people_lead_flag
		, workday.worker_type
		, workday.cost_center_id
		, workday.team_code
		, workday.job_profile_set
	FROM operation_agents_with_cases_scored AS ops_agents
	INNER JOIN registry.raw_oltp.users AS registry_user
		ON ops_agents.registry_id = registry_user.uid
	INNER JOIN sqoffice_data.public.wd_employees AS workday
		ON registry_user.employee_id = workday.employee_id
	WHERE
		1=1
	AND 
		people_lead_flag = 0
),
ops_agents_direct_lead AS (
SELECT DISTINCT 
	ops_agents.ops_agent_employee_id
	, max_by(manager_hierarchy.lead_employee_id, manager_hierarchy.effective_start) AS current_direct_lead_employee_id 
	, max_by(manager_hierarchy.direct_lead, manager_hierarchy.effective_start) AS current_direct_lead
	, max_by(manager_hierarchy.skip_lead, manager_hierarchy.effective_start)   AS current_skip_lead
FROM square_compliance_quality_assurance_operation_agents AS ops_agents
INNER JOIN sqoffice_data.public.wd_manager_hierarchy_history AS manager_hierarchy
	ON ops_agents.ops_agent_employee_id = manager_hierarchy.employee_id
WHERE manager_hierarchy.direct_lead IS NOT NULL
GROUP BY ALL  
),
ops_agent_direct_lead_email AS (
SELECT
	lead.ops_agent_employee_id
	, lead.current_direct_lead_employee_id 
	, lead.current_direct_lead
	, lead.current_skip_lead
	, lead_workday.email AS current_direct_lead_workday_email
	, CASE 
		WHEN current_direct_lead_workday_email LIKE '%@squareup.com' THEN REPLACE(current_direct_lead_workday_email, '@squareup.com', '@block.xyz')
		WHEN current_direct_lead_workday_email LIKE '%@block.xyz' THEN REPLACE(current_direct_lead_workday_email, '@block.xyz', '@squareup.com')
		ELSE current_direct_lead_workday_email
	END AS current_direct_lead_alternate_email
FROM ops_agents_direct_lead AS lead
LEFT JOIN sqoffice_data.public.wd_employees AS lead_workday
	ON lead.current_direct_lead_employee_id = lead_workday.employee_id
),
headcount_category_dri AS (
SELECT DISTINCT
    headcount_category
    , TRIM(VALUE) AS ops_dri 
FROM app_compliance.seller.square_queue_metadata, 
LATERAL SPLIT_TO_TABLE(ops_dri, '&')
WHERE headcount_category <> 'Unknown'
),
recent_cases AS (
SELECT 
	headcount_category_queue 
	, closing_agent_name 
	, closing_agent_uid
	, COUNT(DISTINCT queue) AS past_one_month_queue_count  
	, ROW_NUMBER() OVER (PARTITION BY closing_agent_uid ORDER BY COUNT(DISTINCT queue) DESC) row_num 
FROM app_compliance.square.compliance_agent_cases_history
WHERE 
	DATE_TRUNC(month, most_recent_closed_case_date) BETWEEN DATEADD(month,-1,DATE_TRUNC(month,current_date())) AND current_date()
GROUP BY ALL
QUALIFY ROW_NUMBER() OVER (PARTITION BY closing_agent_uid ORDER BY COUNT(DISTINCT queue) DESC) = 1 
ORDER BY 1
),
square_quality_assurance_operation_agent_details AS (
SELECT
	ops_agents.ops_agent_employee_id
	, ops_agents.registry_id 
	, ops_agents.employee_name
	, ops_agents.workday_email
	, ops_agents.alternate_email
	, ops_agents.active_status
	, ops_agents.city
	, ops_agents.state
	, ops_agents.country
	, ops_agents.location
	, ops_agents.hire_date
	, ops_agents.termination_date
	, ops_agents.people_lead_flag
	, ops_agents.worker_type
	, ops_agents.cost_center_id
	, ops_agents.team_code
	, ops_agents.job_profile_set
	, direct_lead.current_direct_lead_employee_id 
	, direct_lead.current_direct_lead
	, direct_lead.current_skip_lead
	, direct_lead.current_direct_lead_workday_email
	, direct_lead.current_direct_lead_alternate_email
	, agent_headcount_category.headcount_category AS headcount_category_workday
	, pos_cases.headcount_category_queue AS headcount_category_pos_cases
	, CASE 
		WHEN headcount_category_pos_cases = 'Quality Management' THEN headcount_category_pos_cases
		WHEN ops_agents.location <> 'FTE Internal' THEN headcount_category_pos_cases 
		WHEN headcount_category_pos_cases IS NOT NULL AND headcount_category_pos_cases != headcount_category_workday THEN headcount_category_pos_cases 
		ELSE headcount_category_workday 
	END AS headcount_category
FROM
	square_compliance_quality_assurance_operation_agents AS ops_agents
LEFT JOIN ops_agent_direct_lead_email AS direct_lead
	ON ops_agents.ops_agent_employee_id = direct_lead.ops_agent_employee_id
LEFT JOIN headcount_category_dri AS agent_headcount_category	
	ON agent_headcount_category.ops_dri = direct_lead.current_direct_lead
LEFT JOIN recent_cases as pos_cases 
	ON pos_cases.closing_agent_uid = ops_agents.registry_id	
),
final AS (
SELECT 
	ops_agent_employee_id
	, registry_id 
	, employee_name
	, workday_email
	, alternate_email
	, active_status
	, city
	, state
	, country
	, location
	, hire_date
	, termination_date
	, people_lead_flag
	, worker_type
	, cost_center_id
	, team_code
	, job_profile_set
	, current_direct_lead_employee_id 
	, current_direct_lead
	, current_skip_lead
	, current_direct_lead_workday_email
	, current_direct_lead_alternate_email
	, headcount_category
FROM
	square_quality_assurance_operation_agent_details
)
SELECT * FROM final