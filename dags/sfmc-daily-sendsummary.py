import datetime, os, re

from airflow import DAG
from airflow.contrib.kubernetes import pod
from airflow.contrib.kubernetes import secret, volume, volume_mount
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable


#PROJECT          = Variable.get('gcp_project')
PROJECT          = os.environ.get('gcp_project')
LATEST_DI_TAG    = Variable.get('latest_di_image_tag')
LATEST_BQ_TAG    = Variable.get('latest_queries_tag')
INCOMING_BUCKET  = PROJECT + '-data-incoming'
SPARKJOBS_BUCKET = PROJECT + '-data-sparkjobs'
OUTPUT_BUCKET    = PROJECT + '-data-sfmc'
QUERIES_BUCKET   = PROJECT + '-data-etl-queries'

IMAGE            = 'gcr.io/' + PROJECT + '/data-integrations:' + LATEST_DI_TAG
QUERIES_IMAGE    = 'gcr.io/' + PROJECT + '/bigquery-queries:'  + LATEST_BQ_TAG

sf_ftp_user = secret.Secret( deploy_type='env',
    deploy_target='SALESFORCE_FTP_USERNAME',
    secret='di-secrets',
    key='SALESFORCE_FTP_USERNAME')
sf_ftp_pass = secret.Secret( deploy_type='env',
    deploy_target='SALESFORCE_FTP_PASSWORD',
    secret='di-secrets',
    key='SALESFORCE_FTP_PASSWORD')

default_args = {
          'retries': 3,
          'retry_delay': datetime.timedelta(seconds=10),
          'retry_exponential_backoff': True,
          'max_retry_delay': datetime.timedelta(minutes=10),
          #'end_date': datetime.datetime(2019,2,16),
          'start_date': datetime.datetime(2019,4,18)
          # TODO: email stuff
          #'catchup': False,
}

dag = DAG(dag_id='sfmc-daily-sendsummary',
          schedule_interval=datetime.timedelta(days=1),
          default_args=default_args
         )

sf_ftp_fetch = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='sf-ftp-daily-sendsummary-fetcher',
        name='sf-ftp-daily-sendsummary-fetcher',
        namespace='default',
        image=IMAGE,
        secrets=[sf_ftp_user, sf_ftp_pass],
        cmds=['sh', '-c', '/usr/local/bin/salesforce_ftp.py --dest-dir /tmp/ --date {{ ds }} && gsutil cp -r /tmp/{{ ds }} gs://' + PROJECT + '-data-incoming/sfmc/sendsummary/'],
        #cmds=['sh', '-c', 'sleep 7200'],
        dag=dag)

cleanup_op = BashOperator(
               task_id='sfmc-daily-sendsummary-load-cleanup',
               #bash_command='gsutil rm -r gs://'+ INCOMING_BUCKET +'/sfmc/sendsummary/{{ ds }}',
               bash_command='echo "NOOP"',
               dag=dag)

stage_dess = BashOperator(
                  task_id='sfmc-daily-sendsummary-load',
                  bash_command='bq load --location=US --source_format CSV --skip_leading_rows=1 --replace sfmc_staging.daily_email_send_summary gs://' + INCOMING_BUCKET + '/sfmc/sendsummary/{{ ds }}/DailyEmailSendSummary*.csv',
                  dag=dag)

stage_dess.set_upstream(sf_ftp_fetch)

query_location = 'gs://' + QUERIES_BUCKET + '/sfmc/daily_email_send_summary_merge.sql'

merge_dess = kubernetes_pod_operator.KubernetesPodOperator(
              task_id='sfmc-daily-sendsummary-merge',
              name='sfmc-daily-sendsummary-merge',
              namespace='default',
              image=QUERIES_IMAGE,
              cmds=['bash', '-c', '/usr/local/bin/run_query.py -a query -q ' + query_location],
              #cmds=['sh', '-c', 'sleep 3600'],
              dag=dag)

merge_dess.set_upstream(stage_dess)
merge_dess.set_downstream(cleanup_op)

# POSSIBLE TODO:
#   Put this query somewhere *not* in the DAG?
#   Maybe create a repo with all the queries in it, then
#   build a docker container from it, and call that from here.
#
#merge_query = """
#  MERGE sfmc.daily_email_send_summary P
#  USING sfmc_staging.daily_email_send_summary S
#  ON P.send_date=CAST(S.send_date AS DATE) AND P.message_id=CAST(S.message_id AS STRING)
#  WHEN NOT MATCHED THEN
#    INSERT (send_date,
#            message_id,
#            email_name,
#            sends,
#            deliveries,
#            delivery_rate,
#            unique_opens,
#            open_rate,
#            unique_clicks,
#            click_rate,
#            unsubscribes,
#            unsubscribe_rate,
#            complaints) 
#    VALUES ( CAST(send_date AS DATE),
#             message_id,
#             email_name,
#             CAST(REPLACE(sends, ',', '') AS INT64),
#             CAST(REPLACE(deliveries, ',', '') AS INT64),
#             delivery_rate,
#             CAST(REPLACE(unique_opens, ',', '') AS INT64),
#             open_rate,
#             CAST(REPLACE(unique_clicks, ',', '') AS INT64),
#             click_rate,
#             CAST(REPLACE(unsubscribes, ',', '') AS INT64),
#             unsubscribe_rate,
#             CAST(REPLACE(complaints, ',', '') AS INT64));
#"""
#
#merge_query = re.sub("\n| {2,}", ' ', merge_query)
#if re.search('"', merge_query):
#  raise Exception("You must not use double quotes in your query!")
#
#merge_dess = BashOperator(
#                  task_id='sfmc-daily-sendsummary-merge',
#                  bash_command='bq query --use_legacy_sql=FALSE "%s"' % merge_query,
#                  dag=dag)
#
#stage_dess.set_upstream(sf_ftp_fetch)
#merge_dess.set_upstream(stage_dess)
#merge_dess.set_downstream(cleanup_op)
