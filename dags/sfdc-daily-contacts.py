import datetime, re, os

from airflow import DAG
from airflow.contrib.kubernetes import pod
from airflow.contrib.kubernetes import secret, volume, volume_mount
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable


PROJECT          = os.environ.get('gcp_project')
LATEST_DI_TAG    = Variable.get('latest_di_image_tag')
LATEST_BQ_TAG    = Variable.get('latest_queries_tag')
INCOMING_BUCKET  = PROJECT + '-data-incoming'
SPARKJOBS_BUCKET = PROJECT + '-data-sparkjobs'
OUTPUT_BUCKET    = PROJECT + '-data-sfdc'
QUERIES_BUCKET   = PROJECT + '-data-etl-queries'

DI_IMAGE         = 'gcr.io/' + PROJECT + '/data-integrations:' + LATEST_DI_TAG
QUERIES_IMAGE    = 'gcr.io/' + PROJECT + '/bigquery-queries:' + LATEST_BQ_TAG

sf_secret = secret.Secret( deploy_type='volume',
    deploy_target='/config',
    secret='salesforce-fetcher-config',
    key='salesforce_fetcher_config.yaml')

default_args = {
          'retries': 5,
          'retry_delay': datetime.timedelta(seconds=10),
          'retry_exponential_backoff': True,
          'max_retry_delay': datetime.timedelta(minutes=10),
          #'end_date': datetime.datetime(2019,2,19),
          'start_date': datetime.datetime(2019,4,19),
          'catchup_by_default': False,
          # TODO: email stuff
}

dag = DAG(dag_id='sfdc-daily-contacts',
          schedule_interval=datetime.timedelta(days=1),
          catchup=False,
          default_args=default_args
         )

fetch_op   = {}
clean_op   = {}
load_op    = {}
queries_op = {}

for report in ['contacts']:

  # stupid airflow not sanitizing input
  report_name_for_k8s = re.sub('_', '-', report)

  fetch_op[report] = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='sfdc-daily-fetcher-' + report_name_for_k8s,
        name='sfdc-daily-fetcher-'+ report_name_for_k8s,
        namespace='default',
        image=DI_IMAGE,
        secrets=[sf_secret],
        cmds=['sh', '-c', 'export LC_ALL=C.UTF-8; export LANG=C.UTF-8; /usr/local/bin/salesforce-fetcher --config-file /config/salesforce_fetcher_config.yaml --fetch-only ' + report + ' && gsutil cp -r /tmp/salesforce_fetcher/* gs://'+INCOMING_BUCKET+'/sfdc/'],
        #cmds=['sh', '-c', 'sleep 3600'],
        dag=dag) 

# arrrggghhh, the fetch op cannot run historically (since the query only pulls current data),
# and then bad data will be loaded into the SUMMARY table.
# Decouple the fetch op from the rest of the ops? Make it a separate thingy with no catchup?

  load_op[report] = BashOperator(
                  task_id='sfdc-daily-load-' + report_name_for_k8s,
                  bash_command='bq load --location=US --source_format CSV --skip_leading_rows=1 ' +  \
                               '--autodetect --replace sfdc.'+ report + ' gs://' + INCOMING_BUCKET + \
                               '/sfdc/' + report + '/{{ macros.ds_add(ds, 1) }}/output.csv',
                  dag=dag)

  load_op[report].set_upstream(fetch_op[report])

  queries_op[report] = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='sfdc-daily-queries-' + report_name_for_k8s,
        name='sfdc-daily-queries-' + report_name_for_k8s,
        namespace='default',
        image=QUERIES_IMAGE,
        cmds=['bash', '-c', '/usr/local/bin/sfdc_contacts_queries.sh {{ macros.ds_add(ds, 1) }} '+QUERIES_BUCKET],
        #cmds=['sh', '-c', 'sleep 3600'],
        dag=dag)

  queries_op[report].set_upstream(load_op[report])

  # The fetcher seems to report failure even when everything succeeds. Perhaps because airflow loses
  # its connection to the Pod? Shrug. Anyway, setting the trigger_rule for the next task to 'all_done'
  # will cause it to run even if the fetcher reports "failure"
  # Ah, known issue: https://cloud.google.com/composer/docs/how-to/using/using-kubernetes-pod-operator#task_fails_despite_pod_success
  #
  #  bash_op[report] = BashOperator(
  #                  trigger_rule='all_done',
  #                  task_id='sfdc-daily-load-' + report_name_for_k8s,
  #                  bash_command='gcloud dataproc jobs submit pyspark gs://'+SPARKJOBS_BUCKET+'/etl/sfdc_loader.py --cluster=etl-cluster --region us-central1 -- gs://'+INCOMING_BUCKET+'/sfdc/' + report + '/{{ macros.ds_add(ds, 1) }} ' + report + ' gs://'+OUTPUT_BUCKET,
  #                  dag=dag)
  #
  #  bash_op[report].set_upstream(fetch_op[report])

  # Do we need to cleanup? No PII here.
  clean_op[report] = BashOperator(
               task_id='sfdc-daily-load-cleanup-' + report_name_for_k8s,
               #bash_command='gsutil rm gs://'+INCOMING_BUCKET+'/sfdc/' + report + '/{{ macros.ds_add(ds, 1) }}',
               bash_command='echo "NOOP"',
               dag=dag)

  clean_op[report].set_upstream(load_op[report])

