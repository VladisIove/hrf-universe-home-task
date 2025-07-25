DAYS_TO_HIRE_STATISTICS = {
    200: {
        "content": {
            "application/json": {
                "example": {
                    "standard_job_id": "5affc1b4-1d9f-4dec-b404-876f3d9977a0",
                    "country_code": "DE",
                    "min_days": 11.0,
                    "avg_days": 50.5,
                    "max_days": 80.9,
                    "job_postings_number": 100,
                }
            }
        }
    },
    404: {"content": {"application/json": {"example": "Statistics not found"}}},
    504: {
        "content": {
            "application/json": {
                "example": "Oooops...Smth go wrong, our developers already working on this issue."
            }
        }
    },
}
