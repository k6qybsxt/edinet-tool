def validate_runtime_before_batch(jobs, runtime):
    if len(jobs) > runtime.max_companies:
        raise ValueError(f"jobs exceeds max_companies: {len(jobs)} > {runtime.max_companies}")

    seen = set()
    for job in jobs:
        key = (
            job.get("company_code"),
            job.get("file1"),
            job.get("file2"),
            job.get("file3"),
        )
        if key in seen:
            raise ValueError(f"duplicate job detected: {key}")
        seen.add(key)