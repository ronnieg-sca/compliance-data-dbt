WITH column_transforms_and_renames AS (
	SELECT
	-- ids/tokens/keys
		qc_decision_id AS decision_id,
		qc_question_id AS question_id,

	-- dates/timestamps
		created_at AS question_created_at,
		updated_at AS question_updated_at,

	-- booleans

	-- measures
		points,

	-- strings/varchars
		answer,
		keyword,

	-- metadata
		__cdc_create_timestamp,
		__cdc_update_timestamp,
		__db_name,
		__deleted_upstream,
		__entity_schema_name,
	FROM 
		merchantcomphub.raw_oltp.qc_question_answer
),
final AS (
	SELECT
		decision_id,
		question_id,
		question_created_at,
		question_updated_at,
		points,
		answer,
		keyword,
	FROM 
		column_transforms_and_renames
)
SELECT * FROM final