import logging
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select

from home_task.db import get_session
from home_task.models import DaysToHire

from hrf_universe_home_task.query_params import DayToHireStatisticsQueryParams
from hrf_universe_home_task.response_documentation import DAYS_TO_HIRE_STATISTICS


router = APIRouter(prefix="/stats")


logger = logging.getLogger()
logger.setLevel(logging.INFO)


@router.get(
    "/days_to_hire",
    description='Return "days to hire" statistics.',
    tags=[
        "Statistic",
    ],
    responses=DAYS_TO_HIRE_STATISTICS,
)
def get_days_to_hire_stats(
    params: DayToHireStatisticsQueryParams = Depends(),
) -> DaysToHire:
    """Get hiring statistics for a specific job and optionally a specific country.

    Args:
        standard_job_id: ID of the standard job to get statistics for
        country_code: Optional country code to filter statistics by

    Returns:
        Dictionary containing min, max, average days to hire and number of job postings

    Raises:
        HTTPException: If statistics are not found or on server error
    """

    with get_session() as session:
        try:
            query = select(DaysToHire).where(
                DaysToHire.standard_job_id == params.standard_job_id,
                DaysToHire.country_code == params.country_code,
            )
            result = session.execute(query).first()
        except Exception as e:
            logger.error(e, exc_info=True)
            raise HTTPException(
                status_code=504,
                detail="Oooops...Smth go wrong, our developers already working on this issue.",
            )

        if not result:
            raise HTTPException(status_code=404, detail="Statistics not found")

        return result[0]
