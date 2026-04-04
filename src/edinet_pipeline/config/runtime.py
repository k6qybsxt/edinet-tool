from dataclasses import dataclass


@dataclass(frozen=True)
class RuntimeConfig:
    max_companies: int = 50
    parse_cache_max_items: int = 16
    cleanup_retry_count: int = 10
    cleanup_retry_wait_sec: float = 1.0
    use_process_pool: bool = True
    max_workers: int = 4
    write_company_jobs_csv: bool = True
    write_raw_sheet: bool = True
    enable_stock: bool = True
