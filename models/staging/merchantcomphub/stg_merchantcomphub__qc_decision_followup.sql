WITH column_transforms_and_renames AS (
	SELECT
	-- ids/tokens/keys
		id as follow_up_action_id,
		decision_id,

	-- dates/timestamps
		created_at AS follow_up_action_created_at,

	-- booleans
	-- measures

	-- strings/varchars
		agent_alias,

	-- metadata
		__cdc_create_timestamp,
		__cdc_update_timestamp,
		__db_name,
		__deleted_upstream,
		__entity_schema_name,
	FROM 
		merchantcomphub.raw_oltp.qc_decision_followup
	WHERE
		-- filters out QA test and sample data pre-launch date of Aug 1, 2024
		follow_up_action_created_at::date >= '2024-08-01'
),
final AS (
	SELECT
		follow_up_action_id,
		decision_id,
		follow_up_action_created_at,
		agent_alias,
	FROM 
		column_transforms_and_renames
)
SELECT * FROM final