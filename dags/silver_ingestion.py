from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from pathlib import Path
import datetime

CONN_ID = "warehouse_dummy"
SQL_DIR = Path(__file__).parent / "sql" / "quality"
REJECT_THRESHOLD = 0.05
import pendulum

@dag(start_date=pendulum.datetime(2021, 1, 1, tz="Europe/Paris"),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
)
def ingest_silver():

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables_silver",
        conn_id=CONN_ID,
        sql="sql/create_silver_table.sql",
    )

    insert_products_silver = SQLExecuteQueryOperator(
        task_id="insert_silver_products",
        conn_id=CONN_ID,
        sql="sql/insert_silver_products.sql",
    )

    insert_carts_silver = SQLExecuteQueryOperator(
        task_id="insert_silver_carts",
        conn_id=CONN_ID,
        sql="sql/insert_silver_cart_items.sql",
    )

    insert_cart_items_silver = SQLExecuteQueryOperator(
        task_id="insert_silver_cart_items",
        conn_id=CONN_ID,
        sql="sql/insert_silver_cart_items.sql",
    )

    @task
    def run_quality_check(script_name: str, target_table: str, **context) -> dict:
        sql = (SQL_DIR / script_name).read_text(encoding="utf-8")
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        run_id = context["run_id"]

        violations = hook.get_records(sql)          # (rule_name, severity, entity_key, details)
        total_rows = hook.get_first(f"SELECT COUNT(*) FROM {target_table}")[0]

        blocking = [v for v in violations if v[1] == "blocking"]
        rejects  = [v for v in violations if v[1] == "reject"]
        warnings = [v for v in violations if v[1] == "warning"]

        # Journal des controles : une ligne par regle violee, pour la tracabilite
        if violations:
            hook.insert_rows(
                table="quality_check_log",
                rows=[(run_id, target_table, v[0], v[1], v[2], v[3]) for v in violations],
                target_fields=["run_id", "target_table", "rule_name",
                               "severity", "entity_key", "details"],
                commit_every=1000,
            )

        # Quarantaine des violations ligne a ligne
        if rejects:
            hook.insert_rows(
                table="quality_rejects",
                rows=[(run_id, target_table, v[0], v[2], v[3]) for v in rejects],
                target_fields=["run_id", "target_table", "rule_name",
                               "entity_key", "details"],
                commit_every=1000,
            )

        reject_rate = len(rejects) / total_rows if total_rows else 0

        if blocking:
            raise AirflowException(
                f"{target_table} : {len(blocking)} violation(s) bloquante(s) "
                f"-> {', '.join(sorted({v[0] for v in blocking}))}"
            )
        if reject_rate > REJECT_THRESHOLD:
            raise AirflowException(
                f"{target_table} : taux de rejet {reject_rate:.1%} "
                f"au-dessus du seuil {REJECT_THRESHOLD:.0%} ({len(rejects)}/{total_rows})"
            )

        return {
            "table": target_table,
            "rejets": len(rejects),
            "avertissements": len(warnings),
            "taux_rejet": round(reject_rate, 4),
        }

    check_products = run_quality_check.override(task_id="check_silver_products")(
        "check_silver_products.sql", "silver_products"
    )
    check_carts = run_quality_check.override(task_id="check_silver_carts")(
        "check_silver_carts.sql", "silver_carts"
    )
    check_cart_items = run_quality_check.override(task_id="check_silver_cart_items")(
        "check_silver_cart_items.sql", "silver_cart_items"
    )

    create_tables >> [insert_products_silver, insert_carts_silver] >> insert_cart_items_silver

    insert_products_silver >> check_products
    insert_carts_silver    >> check_carts
    insert_cart_items_silver >> check_cart_items


ingest_silver()