from airflow.operators.email_operator import EmailOperator
from airflow.utils.email import send_email
from airflow.models import Variable

def task_failure_callback(context):
    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    exception = context.get('exception')

    subject = f"Airflow Task Failure: {task_instance.task_id}"
    html_content = f"""
    <h3>Task Failure</h3>
    <p>Task <strong>{task_instance.task_id}</strong> in DAG <strong>{dag_run.dag_id}</strong> failed.</p>
    <p>Exception: {exception}</p>
    """

    send_email(
        to=Variable.get("alert_email"),
        subject=subject,
        html_content=html_content
    )