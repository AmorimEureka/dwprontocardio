

from cosmos import ProfileConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

airflow_postgres_db = ProfileConfig(
    profile_name="dbt_prontocardio",
    target_name="prod",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_prontocardio",
        profile_args={"schema":"raw_mv"},
    ),
)