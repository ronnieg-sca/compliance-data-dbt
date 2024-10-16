WITH decision_procedure_review_source AS (
	SELECT * FROM merchantcomphub.raw_oltp.qc_decision_procedure_review
),
final AS (
SELECT

	-- ids/tokens/keys
	id
	, decision_id

	-- dates/timestamps
	, created_at

	-- booleans
	-- measures

	-- strings/varchars
	, agent_alias

	-- metadata
	, __cdc_create_timestamp
	, __cdc_update_timestamp
	, __db_name
	, __entity_schema_name
	, __deleted_upstream

FROM
	decision_procedure_review_source
)
SELECT * FROM final
;