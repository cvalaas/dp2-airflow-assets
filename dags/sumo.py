import datetime
import logging
import os

from airflow import models
from airflow.contrib.kubernetes import pod
from airflow.contrib.kubernetes import secret, volume, volume_mount
from airflow.contrib.operators import kubernetes_pod_operator


YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
IMAGE='gcr.io/imposing-union-227917/sumo-image18:8d605ba'

# expose secrets as environment variables
sg_token = secret.Secret(
    deploy_type='env',
    deploy_target='SUMO_SURVEYGIZMO_TOKEN',
    secret='sumo-secrets',
    key='SUMO_SURVEYGIZMO_TOKEN')
sg_key = secret.Secret(
    deploy_type='env',
    deploy_target='SUMO_SURVEYGIZMO_KEY',
    secret='sumo-secrets',
    key='SUMO_SURVEYGIZMO_KEY')

logger = logging.getLogger(__name__)

with models.DAG(
        dag_id='sumo-data-v2',
        schedule_interval=datetime.timedelta(days=1),
        start_date=datetime.datetime(2019,2,23)) as dag:

    surveygizmo_kubernetes_pod_v2 = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='sumo-data-surveygizmo-v2',
        name='sumo-data-surveygizmo-v2',
        namespace='default',
        image=IMAGE,
        # Entrypoint of the container, if not specified the Docker container's
        # entrypoint is used. The cmds parameter is templated.
        #cmds=['python3', '-m', "SurveyGizmo.run_get_survey_data --outdir='gs://moz-it-data-sumo/surveygizmo' && gsutil cp /tmp/*.csv gs://moz-it-data-sumo/tmp/"],
        cmds=['python3'],
        #cmds=['pip3'],
        #arguments=['list'],
        # Arguments to the entrypoint. The docker image's CMD is used if this
        # is not provided. The arguments parameter is templated.
        arguments=['-m','SurveyGizmo.run_get_survey_data', '--outdir', 'gs://moz-it-data-sumo/surveygizmo'],
        # The secrets to pass to Pod, the Pod will fail to create if the
        # secrets you specify in a Secret object do not exist in Kubernetes.
        secrets=[sg_token,sg_key],
        startup_timeout_seconds=600,
        # If true, logs stdout output of container. Defaults to True.
		get_logs=True,
        dag=dag)
