import logging
import argparse
from psycopg2 import connect, sql

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class CalculateDaysToHireJob:

    def __init__(
        self,
        rds_db_name: str,
        rds_db_username: str,
        rds_db_password: str,
        rds_host: str,
        rds_port: str,
    ) -> None:
        self._rds_db_name = rds_db_name
        self._rds_db_username = rds_db_username
        self._rds_db_password = rds_db_password
        self._rds_host = rds_host
        self._rds_port = rds_port

    @staticmethod
    def __get_temp_table_name(table_name: str) -> str:
        return f"temp_{table_name}"

    def _get_psycopg2_db_connection(self):
        return connect(
            dbname=self._rds_db_name,
            user=self._rds_db_username,
            password=self._rds_db_password,
            host=self._rds_host,
            port=self._rds_port,
        )

    def _get_sql_to_create_temp_table(self, table_name: str) -> sql.SQL:
        return sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING ALL);").format(
            sql.Identifier(self.__get_temp_table_name(table_name)),
            sql.Identifier(table_name),
        )

    def _get_sql_to_processing_days_to_hire_calculation(
        self,
        table_name: str,
        job_posting_table_name: str = "job_posting",
        job_posting_min: int = 5,
    ) -> sql.SQL:
        _sql = sql.SQL(
            """
            WITH 
                base_data AS (
                    SELECT
                        standard_job_id,
                        country_code,
                        days_to_hire
                    FROM {}
                    WHERE days_to_hire IS NOT NULL
                ),
                percentiles AS (
                    SELECT
                        standard_job_id,
                        country_code,
                        PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY days_to_hire) AS p10,
                        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY days_to_hire) AS p90
                    FROM base_data
                    GROUP BY standard_job_id, country_code
                ),
                filtered AS (
                    SELECT
                        b.standard_job_id,
                        b.country_code,
                        b.days_to_hire,
                        p.p10,
                        p.p90
                    FROM base_data b
                    JOIN percentiles p ON
                        b.standard_job_id = p.standard_job_id 
                    WHERE b.days_to_hire > p.p10 AND b.days_to_hire < p.p90 AND b.country_code is not Null
                ),
                aggregated_country AS (
                    SELECT
                        standard_job_id,
                        country_code,
                        COUNT(*) AS job_postings_number,
                        AVG(days_to_hire)::INT AS avg_days,
                        min(p10) as min_days,
                        max(p90) as max_days
                    FROM filtered
                    GROUP BY standard_job_id, country_code

                ),
                world_percentiles AS (
                    SELECT
                        standard_job_id,
                        PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY days_to_hire) AS p10,
                        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY days_to_hire) AS p90
                    FROM base_data
                    GROUP BY standard_job_id
                ),
                filtered_world AS (
                    SELECT
                        b.standard_job_id,
                        b.days_to_hire,
                        p.p10,
                        p.p90
                    FROM base_data b
                    JOIN world_percentiles p ON b.standard_job_id = p.standard_job_id
                    WHERE b.days_to_hire > p.p10 AND b.days_to_hire < p.p90
                ),
                aggregated_world AS (
                    SELECT
                        standard_job_id,
                        NULL AS country_code,
                        COUNT(*) AS job_postings_number,
                        AVG(days_to_hire)::INT AS avg_days,
                        min(p10) as min_days,
                        max(p90) as max_days
                    FROM filtered_world
                    GROUP BY standard_job_id

                ),
                final_result AS (
                    SELECT * FROM aggregated_country
                    UNION ALL
                    SELECT * FROM aggregated_world
                )
                INSERT INTO {} (
                    id,
                    standard_job_id,
                    country_code,
                    job_postings_number,
                    avg_days,
                    min_days,
                    max_days
                )
                SELECT 
                    ROW_NUMBER() OVER () AS id,
                    standard_job_id,
                    country_code,
                    job_postings_number,
                    avg_days,
                    min_days,
                    max_days
                FROM final_result
                WHERE job_postings_number > {};
            """
        ).format(
            sql.Identifier(job_posting_table_name),
            sql.Identifier(self.__get_temp_table_name(table_name)),
            sql.Literal(job_posting_min),
        )
        return _sql

    def _get_sql_to_drop_old_table(self, table_name: str) -> sql.SQL:
        return sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(
            sql.Identifier(table_name)
        )

    def _get_sql_to_rename_new_table(self, table_name: str) -> sql.SQL:
        return sql.SQL("ALTER TABLE {} RENAME TO {};").format(
            sql.Identifier(self.__get_temp_table_name(table_name)),
            sql.Identifier(table_name),
        )

    def run(
        self,
        table_name: str = "days_to_hire",
        job_posting_table_name: str = "job_posting",
        job_posting_min: str = 5,
    ):
        connection = self._get_psycopg2_db_connection()
        connection.autocommit = False

        create_temp_table_sql = self._get_sql_to_create_temp_table(table_name)
        processing_sql = self._get_sql_to_processing_days_to_hire_calculation(
            table_name, job_posting_table_name, job_posting_min
        )
        delete_old_table_sql = self._get_sql_to_drop_old_table(table_name)
        rename_new_table_sql = self._get_sql_to_rename_new_table(table_name)

        cursor = connection.cursor()
        try:
            cursor.execute(create_temp_table_sql)
            cursor.execute(processing_sql)
            cursor.execute(delete_old_table_sql)
            cursor.execute(rename_new_table_sql)
            connection.commit()
        except Exception as e:
            connection.rollback()
            logger.error(e, exc_info=True)
            cursor.close()
            connection.close()
            raise
        finally:
            cursor.close()
            connection.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Process and store job posting statistics."
    )

    parser.add_argument(
        "--save_to_table_name",
        type=str,
        default="days_to_hire",
        help="Name of the table where the output data will be saved. (default: days_to_hire)",
    )

    parser.add_argument(
        "--job_posting_table_name",
        type=str,
        default="job_posting",
        help="Name of the source table containing job posting data. (default: job_posting)",
    )

    parser.add_argument(
        "--job_posting_min",
        type=int,
        default=5,
        help="Minimum number of job postings required for a row to be saved in the output. (default: 5)",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # for simplification credentials was been hardcoded
    CalculateDaysToHireJob(
        "home_task", "admin", "adm1n_password", "localhost", "5432"
    ).run(
        args.save_to_table_name,
        args.job_posting_table_name,
        args.job_posting_min,
    )
