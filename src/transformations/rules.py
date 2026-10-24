"""SQL predicates shared by expectations and quarantine routing."""

FAIL_RULES = {
    "valid_id": "_id IS NOT NULL AND length(_id) > 0",
    "known_version": "schema_version = 1",
}

DROP_RULES = {
    "valid_student": "student_id IS NOT NULL AND length(student_id) > 0",
    "valid_school": "school_id IS NOT NULL AND grade_level IS NOT NULL",
    "valid_time": "attempt_timestamp IS NOT NULL AND attempt_timestamp <= current_timestamp() + INTERVAL 5 MINUTES",
    "valid_update_time": "updated_at IS NOT NULL AND updated_at >= attempt_timestamp AND updated_at <= current_timestamp() + INTERVAL 5 MINUTES",
    "valid_product": "product_name IS NOT NULL AND trim(product_name) <> ''",
    "valid_completion": "is_completed IS NOT NULL",
    "no_rescued_fields": "_rescued_data IS NULL",
    "no_corrupt_record": "_corrupt_record IS NULL",
    "valid_change_type": (
        "change_type IS NOT NULL AND change_type IN ('ActivityStarted','ActivityIn_Progress',"
        "'ActivityCompleted','LevelCompleted','UnitCompleted','StrandCompleted')"
    ),
}

WARN_RULES = {
    "grade_in_range": (
        "grade_level IN ('K','01','02','03','04','05','06',"
        "'07','08','09','10','11','12')"
    ),
    "not_future": "attempt_timestamp <= current_timestamp() + INTERVAL 5 MINUTES",
}

DROP_ALL = " AND ".join(f"({rule})" for rule in DROP_RULES.values())

MANIFEST_RULES = {
    "valid_run_id": "run_id IS NOT NULL AND length(run_id) > 0",
    "known_version": "schema_version = 1",
    "valid_counts": "file_count >= 0 AND record_count >= 0",
    "valid_times": "started_at IS NOT NULL AND finished_at >= started_at",
    "path_matches_manifest": "run_id = path_run_id AND path_run_id <> ''",
    "no_rescued_fields": "_rescued_data IS NULL",
}
