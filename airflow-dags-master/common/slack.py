from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


def slack_notification(context, slack_msg):
    failed_alert = SlackWebhookOperator(
        task_id='slack_notification',
        http_conn_id='slack_webhook',
        message=slack_msg)
    return failed_alert.execute(context=context)


def slack_retry_notification(context):
    slack_msg = """
            {channel}
            :large_orange_circle: Task Failed. Retrying... 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        channel='<!channel>' if context.get('task_instance').try_number > 1 else '',
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    slack_notification(context, slack_msg)


def slack_failed_notification(context):
    slack_msg = """
            <!channel>
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    slack_notification(context, slack_msg)


def slack_file_successfully_upload(context, file_url):
    slack_msg = """
            :large_green_circle: File uploaded successfully. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Url*: {file_url}
            """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        exec_date=context.get('execution_date'),
        file_url=file_url,
    )
    slack_notification(context, slack_msg)
