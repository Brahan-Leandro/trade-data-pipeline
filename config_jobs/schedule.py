from dagster import schedule
from .solids import pipeline

@schedule(
    cron_schedule="0 */2 * * *", 
    job=pipeline,
    execution_timezone="America/Bogota"
)
def etl_ops_envios_schedule(context):
    date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return {
        "ops": {
            "load_info_data": {"config": {"execution_date": date}}
        }
    }