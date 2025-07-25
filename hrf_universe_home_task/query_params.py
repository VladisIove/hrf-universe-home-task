from fastapi import Query
from typing import Optional


class DayToHireStatisticsQueryParams:
    def __init__(
        self,
        standard_job_id: str = Query(
            description="Standard job id. UUID format.",
        ),
        country_code: Optional[str] = Query(
            None,
            description="Country code in ISO 3166-1 alpha-2 format. Request without this parameter means that need take global statistics.",
        ),
    ):
        self.standard_job_id = standard_job_id
        self.country_code = country_code
