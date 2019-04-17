import datetime

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

# TODO:
#   1. Create a new data-intgrations image, upload it, and put the hash here
#   2. Add the salesforce secrets to the di-secrets.yaml file and push to k8s
#   3. Add the secrets_salesforceftp.py to the configmap and upload to k8s
#   4. Confirm Sparkjob can accept "DailyEmailSendSummary*" as a filename
#   5. Change sparkjob to handle this file (including the fact that it's CSV not TSV!)

brick_secret = secret.Secret( deploy_type='env',
    deploy_target='BRICKFTP_API_KEY',
    secret='di-secrets',
    key='BRICKFTP_API_KEY')
sf_ftp_user = secret.Secret( deploy_type='env',
    deploy_target='SALESFORCE_FTP_USERNAME',
    secret='di-secrets',
    key='SALESFORCE_FTP_USERNAME')
sf_ftp_pass = secret.Secret( deploy_type='env',
    deploy_target='SALESFORCE_FTP_PASSWORD',
    secret='di-secrets',
    key='SALESFORCE_FTP_PASSWORD')

default_args = {
          'retries': 5,
          'retry_delay': datetime.timedelta(seconds=10),
          'retry_exponential_backoff': True,
          'max_retry_delay': datetime.timedelta(minutes=10),
          #'end_date': datetime.datetime(2019,2,16),
          'start_date': datetime.datetime(2019,2,15)
          # TODO: email stuff
          #'catchup': False,
}

dag = DAG(dag_id='sfmc-daily',
          schedule_interval=datetime.timedelta(days=1),
          default_args=default_args
         )

sfmc_fetch = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='sfmc-daily-fetcher',
        name='sfmc-daily-fetcher',
        namespace='default',
        image=IMAGE,
        secrets=[brick_secret],
        #cmds=['sh', '-c', '/usr/local/bin/brickftp_poc.py --dest-dir /tmp/ --date {{ macros.ds_add(ds, -1) }} && gsutil cp /tmp/{{ macros.ds_add(ds, -1) }} gs://moz-it-data-dp2-incoming-dev/sfmc/'],
        cmds=['sh', '-c', '/usr/local/bin/brickftp_poc.py --dest-dir /tmp/ --date {{ ds }} && gsutil cp -r /tmp/{{ ds }} gs://moz-it-data-dp2-incoming-dev/sfmc/'],
        #cmds=['sh', '-c', 'sleep 360'],
        dag=dag)

sf_ftp_fetch = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='sf-ftp-daily-fetcher',
        name='sf-ftp-daily-fetcher',
        namespace='default',
        image=IMAGE,
        secrets=[sf_ftp_user, sf_ftp_pass],
        cmds=['sh', '-c', '/usr/local/bin/salesforce_ftp.py --dest-dir /tmp/ --date {{ ds }} && gsutil cp -r /tmp/{{ ds }} gs://moz-it-data-dp2-incoming-dev/sfmc/'],
        #cmds=['sh', '-c', 'sleep 360'],
        dag=dag)

cleanup_op = BashOperator(
               task_id='sfmc-daily-load-cleanup',
               bash_command='gsutil rm -r gs://moz-it-data-dp2-incoming-dev/sfmc/{{ ds }}',
               #bash_command='echo "NOOP"',
               dag=dag)

bash_op = {}
for filename in ['Bounces','Clicks','Opens','SendJobs','Sent','Subscribers','Unsubs']:
  bash_op[filename] = BashOperator(
                  task_id='sfmc-daily-load-' + filename,
                  bash_command='gcloud dataproc jobs submit pyspark gs://moz-it-data-dp2-sparkjobs-dev/etl/sfmc_loader.py --cluster=etl-cluster --region us-central1 -- gs://moz-it-data-dp2-incoming-dev/sfmc/{{ ds }} ' + filename + ' gs://moz-it-data-dp2-sfmc-dev',
                  dag=dag)

  bash_op[filename].set_upstream(sfmc_fetch)
  bash_op[filename].set_downstream(cleanup_op)

process_dess = BashOperator(
                  task_id='sfmc-daily-load-DESS',
                  bash_command='gcloud dataproc jobs submit pyspark gs://moz-it-data-dp2-sparkjobs-dev/etl/sfmc_loader.py --cluster=etl-cluster --region us-central1 -- gs://moz-it-data-dp2-incoming-dev/sfmc/{{ ds }} DailyEmailSendSummary* gs://moz-it-data-dp2-sfmc-dev',
                  dag=dag)

process_dess.set_upstream(sf_ftp_fetch)
process_dess.set_downstream(cleanup_op)
