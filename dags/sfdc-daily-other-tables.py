import datetime, re

from airflow import DAG
from airflow.contrib.kubernetes import pod
from airflow.contrib.kubernetes import secret, volume, volume_mount
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

#PROJECT=os.environ.get('gcp_project')

PROJECT=Variable.get('gcp_project')
LATEST_TAG=Variable.get('latest_di_image_tag')

# TODO: put the latest SHA hash in a bucket somewhere and pull from there
IMAGE='gcr.io/' + PROJECT + '/data-integrations:' + LATEST_TAG

sf_secret = secret.Secret( deploy_type='volume',
    deploy_target='/config',
    secret='salesforce-fetcher-config',
    key='salesforce_fetcher_config.yaml')

default_args = {
          'retries': 3,
          'retry_delay': datetime.timedelta(seconds=10),
          'execution_timeout': datetime.timedelta(minutes=30),
          'retry_exponential_backoff': True,
          'max_retry_delay': datetime.timedelta(minutes=10),
          #'end_date': datetime.datetime(2019,2,19),
          'start_date': datetime.datetime(2019,2,26),
          'catchup_by_default': False,
          # TODO: email stuff
}

dag = DAG(dag_id='sfdc-daily-other-tables',
          schedule_interval=datetime.timedelta(days=1),
          catchup=False,
          default_args=default_args
         )

fetch_op = {}
clean_op = {}
bash_op  = {}

for report in ['contact_donor_count','contact_history','donation_record_count','foundation_signups','petition_signups']:

  # stupid airflow not sanitizing input
  report_name_for_k8s = re.sub('_', '-', report)

  fetch_op[report] = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='sfdc-daily-fetcher-' + report_name_for_k8s,
        name='sfdc-daily-fetcher-'+ report_name_for_k8s,
        namespace='default',
        image=IMAGE,
        secrets=[sf_secret],
        cmds=['sh', '-c', 'export LC_ALL=C.UTF-8; export LANG=C.UTF-8; /usr/local/bin/salesforce-fetcher --config-file /config/salesforce_fetcher_config.yaml --fetch-only ' + report + ' && gsutil cp -r /tmp/salesforce_fetcher/* gs://moz-it-data-dp2-incoming-dev/sfdc/'],
        #cmds=['sh', '-c', 'sleep 3600'],
        dag=dag)

  bash_op[report] = BashOperator(
                  task_id='sfdc-daily-load-' + report_name_for_k8s,
                  bash_command='gcloud dataproc jobs submit pyspark gs://moz-it-data-dp2-sparkjobs-dev/etl/sfdc_loader.py --cluster=etl-cluster --region us-central1 -- gs://moz-it-data-dp2-incoming-dev/sfdc/' + report + '/{{ macros.ds_add(ds, 1) }} ' + report + ' gs://moz-it-data-dp2-sfdc-dev',
                  dag=dag)

  bash_op[report].set_upstream(fetch_op[report])

  # Do we need to cleanup? No PII here.
  clean_op[report] = BashOperator(
               task_id='sfdc-daily-load-cleanup-' + report_name_for_k8s,
               #bash_command='gsutil rm gs://moz-it-data-dp2-incoming-dev/sfdc/' + report + '/{{ macros.ds_add(ds, 1) }}',
               bash_command='echo "NOOP"',
               dag=dag)

  clean_op[report].set_upstream(bash_op[report])
