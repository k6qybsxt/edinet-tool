from dataclasses import dataclass


@dataclass
class CompanyTaskResult:
    slot: int | None
    company_code: str
    company_name: str | None
    status: str
    stock_status: str | None = None
    output_excel: str | None = None
    failure_reason: str | None = None
    error_detail: str | None = None
