WITH square_assignments_and_assignment_changes AS (
	SELECT
		changes.assignment_change_id,
		changes.assignment_id,
		changes.occurred_at,
		changes.actor_uid,
		changes.actor_name,
		changes.reason,
		changes.field_name,
		changes.old_value,
		changes.new_value
	FROM
		app_compliance.sca_staging.notary__assignment_changes AS changes
	INNER JOIN
		app_compliance.sca_staging.notary__square_assignments AS assignments ON changes.assignment_id = assignments.assignment_id
),
assignment_status_first_and_last_change AS (
	SELECT
		assignment_id,
		MIN(occurred_at) AS status_first_change_at,
		MAX(occurred_at) AS status_last_change_at
	FROM
		square_assignments_and_assignment_changes
	WHERE
		field_name = 'status'
	GROUP BY ALL
),
current_assignment_status AS (
	SELECT
		assignment_id,
		new_value as current_status,
		occurred_at as status_effective_at
	FROM
		square_assignments_and_assignment_changes
	WHERE
		field_name = 'status'
	GROUP BY ALL
	QUALIFY ROW_NUMBER() OVER (PARTITION BY assignment_id ORDER BY occurred_at ASC) = 1
),
assignment_completed_at AS (
	SELECT
		assignment_id,
		occurred_at as assignment_completed_at
	FROM
		square_assignments_and_assignment_changes
	WHERE
		field_name = 'status'
	AND
		new_value = 'completed'
	GROUP BY ALL
	QUALIFY ROW_NUMBER() OVER (PARTITION BY assignment_id ORDER BY occurred_at DESC) = 1
),
assignment_closed_at AS (
	SELECT
		assignment_id,
		occurred_at AS assignment_closed_at
	FROM
		square_assignments_and_assignment_changes
	WHERE
		field_name = 'status'
	AND
		new_value = 'closed'
	GROUP BY ALL
	QUALIFY ROW_NUMBER() OVER (PARTITION BY assignment_id ORDER BY occurred_at DESC) = 1
),
assignment_first_and_last_claimed_at AS (
	SELECT
		assignment_id,
		min(occurred_at) as first_claimed_at,
		max(occurred_at) as last_claimed_at
	FROM
		square_assignments_and_assignment_changes
	WHERE
		field_name = 'status'
	AND
		new_value = 'claimed'
	GROUP BY ALL
),
assignment_status_details AS (
	SELECT
		assignment_changes.assignment_id,
		current_status.current_status,
		current_status.status_effective_at,
		change.status_first_change_at,
		change.status_last_change_at,
		completed.assignment_completed_at,
		closed.assignment_closed_at,
		claimed.first_claimed_at,
		claimed.last_claimed_at
	FROM
		square_assignments_and_assignment_changes as assignment_changes
	LEFT JOIN 
		current_assignment_status as current_status on assignment_changes.assignment_id = current_status.assignment_id
	LEFT JOIN
		assignment_status_first_and_last_change as change on assignment_changes.assignment_id = change.assignment_id
	LEFT JOIN 
		assignment_completed_at as completed on assignment_changes.assignment_id = completed.assignment_id
	LEFT JOIN 
		assignment_closed_at as closed on assignment_changes.assignment_id = closed.assignment_id
	LEFT JOIN 
		assignment_first_and_last_claimed_at as claimed on assignment_changes.assignment_id = claimed.assignment_id
),
final AS (
	SELECT
		assignment_id,
		current_status,
		status_effective_at,
		status_first_change_at,
		status_last_change_at,
		assignment_completed_at,
		assignment_closed_at,
		first_claimed_at,
		last_claimed_at
	FROM
		assignment_status_details
)
SELECT * FROM final