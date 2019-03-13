import datetime

from airflow import DAG
from airflow.contrib.kubernetes import pod
from airflow.contrib.kubernetes import secret, volume, volume_mount
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators.bash_operator import BashOperator

# TODO: put the latest SHA hash in a bucket somewhere and pull from there
IMAGE='gcr.io/imposing-union-227917/data-integrations:20190307'

brick_secret = secret.Secret( deploy_type='env',
    deploy_target='BRICKFTP_API_KEY',
    secret='di-secrets',
    key='BRICKFTP_API_KEY')

config_volume = volume.Volume(
            name = 'configs',
            configs = {
              'configMap': {
                'name': 'data-integrations-dev',
                'items': [
                  { 'key': 'secrets_brickftp.py', 'path': 'secrets_brickftp.py', },
                  { 'key': 'secrets_util.py',     'path': 'secrets_util.py',     },
                ],
              },
            }
         )
config_mount = volume_mount.VolumeMount(
    name = 'configs',
    mount_path = '/configs',
    sub_path=None,
    read_only = True)

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
        volumes=[config_volume],
        volume_mounts=[config_mount],
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

