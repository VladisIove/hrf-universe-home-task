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

    @staticmethod
    def _build_base_data_table(job_posting_table_name: str) -> sql.SQL:
        return sql.SQL(
            """
             base_data AS (
                    SELECT
                        standard_job_id,
                        country_code,
                        days_to_hire
                    FROM {}
                    WHERE days_to_hire IS NOT NULL
                )
            """
        ).format(sql.Identifier(job_posting_table_name))

    @staticmethod
    def _build_sql_statistic(
        dimensions: list[str] = ["standard_job_id"],
        additional_filters_before_aggregation: str = None,
    ) -> tuple[sql.SQL, str]:

        if additional_filters_before_aggregation is None:
            additional_filters_before_aggregation = sql.SQL("")
        else:
            additional_filters_before_aggregation = sql.SQL(
                additional_filters_before_aggregation
            )

        _group_by = sql.SQL(",".join(dimensions))
        percentiles_table_name = sql.SQL(f'percentiles_{"_".join(dimensions)}')
        filtered_table_name = sql.SQL(f'filtered_{"_".join(dimensions)}')
        aggregated_table_name = sql.SQL(f'aggregated_{"_".join(dimensions)}')
        _sql = sql.SQL(
            """
            {} AS (
                SELECT
                    standard_job_id,
                    PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY days_to_hire) AS p10,
                    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY days_to_hire) AS p90
                FROM base_data
                GROUP BY {}
            ),
            {} AS (
                SELECT
                    b.standard_job_id,
                    b.country_code,
                    b.days_to_hire,
                    p.p10 as min_days,
                    p.p90 as max_days
                FROM base_data b
                JOIN {} p ON b.standard_job_id = p.standard_job_id
                WHERE b.days_to_hire > p.p10 AND b.days_to_hire < p.p90 {}
            ),
            {} AS (
                SELECT
                    {},
                    COUNT(*) AS job_postings_number,
                    AVG(days_to_hire)::INT AS avg_days,
                    min(min_days) as min_days,
                    max(max_days) as max_days
                FROM {}
                GROUP BY {}
            )
        """
        ).format(
            percentiles_table_name,
            _group_by,
            filtered_table_name,
            percentiles_table_name,
            additional_filters_before_aggregation,
            aggregated_table_name,
            _group_by,
            filtered_table_name,
            _group_by,
        )
        return _sql, aggregated_table_name

    @staticmethod
    def _build_final_result_union_table(
        country_aggregated_table_name: str, world_aggregated_table_name: str
    ) -> sql.SQL:
        return sql.SQL(
            """
             final_result AS (
                    SELECT 
                        standard_job_id, country_code, job_postings_number, avg_days, min_days, max_days 
                    FROM {}
                    UNION ALL
                    SELECT 
                        standard_job_id, NULL as country_code, job_postings_number, avg_days, min_days, max_days 
                    FROM {}
                )
            """
        ).format(
            country_aggregated_table_name,
            world_aggregated_table_name,
        )

    @staticmethod
    def _build_inserting_sql(table_name: str, job_posting_min: int) -> sql.SQL:
        return sql.SQL(
            """
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
            sql.Identifier(table_name),
            sql.Literal(job_posting_min),
        )

    def _get_sql_to_processing_days_to_hire_calculation(
        self,
        table_name: str,
        job_posting_table_name: str = "job_posting",
        job_posting_min: int = 5,
    ) -> sql.SQL:

        base_data_sql = self._build_base_data_table(job_posting_table_name)
        world_sql, world_aggregated_table_name = self._build_sql_statistic()
        country_sql, country_aggregated_table_name = self._build_sql_statistic(
            ["standard_job_id", "country_code"], "AND b.country_code is not Null"
        )
        final_result_union_sql = self._build_final_result_union_table(
            country_aggregated_table_name, world_aggregated_table_name
        )
        inserting_sql = self._build_inserting_sql(
            self.__get_temp_table_name(table_name), job_posting_min
        )
        _sql = sql.SQL(
            """
            WITH 
                {},
                {},
                {},
                {}
                {}
            """
        ).format(
            base_data_sql, country_sql, world_sql, final_result_union_sql, inserting_sql
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

    parser.add_argument(
        "--rds_db_name",
        type=str,
        default="home_task",
        help="Database name",
    )

    parser.add_argument(
        "--rds_db_username",
        type=str,
        default="admin",
        help="Username database",
    )

    parser.add_argument(
        "--rds_db_password",
        type=str,
        default="adm1n_password",
        help="Database password",
    )

    parser.add_argument(
        "--rds_host",
        type=str,
        default="localhost",
        help="Database host",
    )

    parser.add_argument(
        "--rds_port",
        type=str,
        default="5432",
        help="Database port",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    CalculateDaysToHireJob(
        args.rds_db_name,
        args.rds_db_username,
        args.rds_db_password,
        args.rds_host,
        args.rds_port,
    ).run(
        args.save_to_table_name,
        args.job_posting_table_name,
        args.job_posting_min,
    )
