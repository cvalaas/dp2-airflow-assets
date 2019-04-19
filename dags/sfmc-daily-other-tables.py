import datetime, os, re

from airflow import DAG
from airflow.contrib.kubernetes import pod
from airflow.contrib.kubernetes import secret, volume, volume_mount
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable


#PROJECT          = Variable.get('gcp_project')
PROJECT          = os.environ.get('gcp_project')
LATEST_TAG       = Variable.get('latest_di_image_tag')
INCOMING_BUCKET  = PROJECT + '-data-incoming'
SPARKJOBS_BUCKET = PROJECT + '-data-sparkjobs'
OUTPUT_BUCKET    = PROJECT + '-data-sfmc'

# TODO: put the latest SHA hash in a bucket somewhere and pull from there
IMAGE='gcr.io/' + PROJECT + '/data-integrations:' + LATEST_TAG

# TODO:
#   1. Create a new data-intgrations image, upload it, and put the hash here - DONE
#   2. Add the salesforce secrets to the di-secrets.yaml file and push to k8s - DONE
#   3. Add the secrets_salesforceftp.py to the configmap and upload to k8s - WONTFIX
#   4. Confirm Sparkjob can accept "DailyEmailSendSummary*" as a filename
#   5. Change sparkjob to handle this file (including the fact that it's CSV not TSV!)

brick_secret = secret.Secret( deploy_type='env',
    deploy_target='BRICKFTP_API_KEY',
    secret='di-secrets',
    key='BRICKFTP_API_KEY')

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

dag = DAG(dag_id='sfmc-daily-other-tables',
          schedule_interval=datetime.timedelta(days=1),
          default_args=default_args
         )

sfmc_fetch = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='sfmc-daily-other-tables-fetcher',
        name='sfmc-daily-other-tables-fetcher',
        namespace='default',
        image=IMAGE,
        secrets=[brick_secret],
        #cmds=['sh', '-c', '/usr/local/bin/brickftp_poc.py --dest-dir /tmp/ --date {{ macros.ds_add(ds, -1) }} && gsutil cp /tmp/{{ macros.ds_add(ds, -1) }} gs://'+INCOMING_BUCKET+'/sfmc/'],
        cmds=['sh', '-c', '/usr/local/bin/brickftp_poc.py --dest-dir /tmp/ --date {{ ds }} && gsutil cp -r /tmp/{{ ds }} gs://' + INCOMING_BUCKET + '/sfmc/other_tables/'],
        #cmds=['sh', '-c', 'sleep 7200'],
        dag=dag)

cleanup_op = BashOperator(
               task_id='sfmc-daily-other-tables-load-cleanup',
               #bash_command='gsutil rm -r gs://'+ INCOMING_BUCKET +'/sfmc/other_tables/{{ ds }}',
               bash_command='echo "NOOP"',
               dag=dag)

bash_op = {}
for filename in ['Bounces','Clicks','Opens','SendJobs','Sent','Subscribers','Unsubs']:
  bash_op[filename] = BashOperator(
                  task_id='sfmc-daily-other-tables-load-' + filename,
                  bash_command='gcloud dataproc jobs submit pyspark gs://'+SPARKJOBS_BUCKET+'/etl/sfmc_loader.py --cluster=etl-cluster --region us-central1 -- gs://'+INCOMING_BUCKET+'/sfmc/other_tables/{{ ds }} ' + filename + ' gs://'+OUTPUT_BUCKET,
                  dag=dag)

  bash_op[filename].set_upstream(sfmc_fetch)
  bash_op[filename].set_downstream(cleanup_op)

