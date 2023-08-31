import os, glob
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from dp_custom_images.image_config import ImageConfig
import json
import requests
import time
from airflow.sensors.python import PythonSensor
from airflow.exceptions import AirflowFailException
import queue
from datetime import datetime, timedelta



from operators.infra_airflow_operators.dataproc_operator_dpaas import (
    DataprocClusterCreateOperatorFromDpaas,
    DataprocClusterDeleteOperatorFromDpaas
)

def create_dag(dag_id, config):
  
  image_project = config.get("projectId")
  image_family = config.get("customFamily")
  
  base_image_project = config.get("baseFamily").split('/')[1]
  base_image_family = config.get("baseFamily").rsplit('/', 1)[1]

  test_image_project = 'wmt-customer-tech-adtech'
  test_image_family = image_family + '-test'
    
  common_image_project = 'wmt-adtech-services'
  common_image_family = image_family

  sa_key = '/etc/secrets/svc-deploy-mgmt.json'
  auth_cmd = f"""
  gcloud config set proxy/type http
  gcloud config set proxy/address sysproxy.wal-mart.com
  gcloud config set proxy/port 8080
  gcloud auth activate-service-account --key-file {sa_key}
  """

  # cluster params
  zone = "us-east4-b"
  region = zone[:-2]
  subnetwork = f"https://www.googleapis.com/compute/v1/projects/shared-vpc-admin/regions/{region}/subnetworks/prod-{region}-01"
  cluster_name = config.get("smokeTest.cluster.name")

  xcom_cmd = f"""
  {auth_cmd}
  dataproc_version=$(gcloud compute images describe-from-family {base_image_family} --project {base_image_project} --format "value(labels.goog-dataproc-version)")
  dp_version=$(echo $dataproc_version | cut -b -3)

  current_time=$(date '+%s')
  test_image_name="{test_image_family}-$current_time"
  image_name="{image_family}-$current_time"

  echo "{{\\"test_image_name\\" : \\"$test_image_name\\", \\"image_name\\" : \\"$image_name\\", \\"dp_version\\" : \\"$dp_version\\" }}" > /airflow/xcom/return.json
  cat /airflow/xcom/return.json
  """

  smoke_test_cmd = f"""
  {auth_cmd}
  cd smoke-test
  if [[ -z "$cluster_name" ]]
  then
    python -c "import sys; import adtech_smoke_test_runner; adtech_smoke_test_runner.run(sys.argv[1],sys.argv[2],sys.argv[3],None,sys.argv[4],sys.argv[5],sys.argv[6],sys.argv[7])" $image_name $project_id $zone $subnetwork $no_external_ip $service_account $dp_version
  else
    python -c "import sys; import adtech_smoke_test_runner; adtech_smoke_test_runner.run_with_cluster(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5])" $image_name $project_id $zone $dp_version $cluster_name
  fi  
  """

  labels_cmd = f"""
  {auth_cmd}
  gcloud compute images add-labels $test_image_name --project {test_image_project} --labels "trproductid=106,app=dataproc,creator=airflow,owner=$owner,jira=$jira_ticket"
  """
  
  promote_image_cmd = f"""
  {auth_cmd}
  cd smoke-test
  python -c "import sys; import smoke_test_utils; smoke_test_utils.copyImage(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5],None)" $source_image $source_image_project $dest_image $dest_image_project $dest_image_family
  """

  def _get_cluster_branch():
    if cluster_name:
        return "create_cluster"
    else:
        return "skip_create_cluster"


  def _get_cluster_sa():
    sa = config.get("smokeTest.cluster.serviceAccount")
    if sa:
      sa = "/etc/secrets/{}.json".format(sa.split('@')[0])
    return sa

  dag = DAG (
      dag_id=dag_id,
      default_args={"start_date": "2022-02-22", "retries": 0},
      schedule_interval=None,
      catchup=False,
      max_active_runs=1
  )
  with dag:

    get_info = KubernetesPodOperator(namespace='adtech-airflow',
      image="docker.prod.walmart.com/adtech-infra/common/utility-helper:latest",
      cmds=["/bin/bash","-c"],
      arguments=[xcom_cmd],
      name="get_info",
      task_id="get-info",
      get_logs=True,
      is_delete_operator_pod=True,
      in_cluster=True,
      do_xcom_push=True
    )    

    create_image = KubernetesPodOperator(
      namespace='adtech-airflow',
        image="docker.prod.walmart.com/adtech-infra/dp-custom-image",
        name="create_image",
        task_id="create-image",
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        env_vars={
          'custom_image_sa': sa_key,
          'project_id': test_image_project,
          'family': test_image_family,
          'image_name': '{{ti.xcom_pull(task_ids="get-info", key="return_value")["test_image_name"]}}',
          'base_image_family': config.get("baseFamily"),
          'customization_script': config.get("customizationScript"),
          'metadata': config.get("metadata"),
          'zone': zone,
          'gcs_bucket': config.get("gcsBucket"),
          'subnetwork': subnetwork,
          'no_external_ip': 'true',
          'shutdown_instance_timer_sec': '10',
          'machine_type': 'n1-highmem-8',
          'disk_size': '50',
          'service_account': 'svc-deploy-mgmt@wmt-customer-tech-adtech.iam.gserviceaccount.com',
          'no_smoke_test': 'true',
          'accelerator': config.get("accelerator")
        }
    )

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=_get_cluster_branch,
    )    

    create_cluster = DataprocClusterCreateOperatorFromDpaas(
        task_id='create_cluster',
        dpaas_args = {
            'projectId': image_project,
            'clusterName': cluster_name,
            'region': region,
            'configs': config.get("smokeTest.cluster.configs"),
            'adhocCluster':{
                'masterConfig': {
                    'image': 'projects/wmt-customer-tech-adtech/global/images/{{ti.xcom_pull(task_ids="get-info", key="return_value")["test_image_name"]}}'
                }
            },
            'serviceAccount': _get_cluster_sa()
        }
    )     

    skip_create_cluster = DummyOperator(task_id="skip_create_cluster")

    def _smoke_test_start(**kwargs):
        smokeexists = True
        remoteexists = True
        data = json.loads(config.get("smokeTest.tests.dag"))
        if len(data) == 0:
            smokeexists = False

        data = json.loads(config.get("smokeTest.tests.gcloud"))
        if len(data) == 0:
            remoteexists = False

        smokeexists = kwargs['dag_run'].conf.get('smokeexists', False)
        remoteexists = kwargs['dag_run'].conf.get('remoteexists', False)
        print("smokeexists:",smokeexists)
        print("remoteexists:",remoteexists)
        if smokeexists and remoteexists:
            result = ["smoke_test", "remote_dag_smoke_test"]
        elif smokeexists and not remoteexists:
            result = "smoke_test"
        elif not smokeexists and remoteexists:
            result = "remote_dag_smoke_test"
        else:
            result = "skip_remote_and_smoke_test"
        print("result",result)
        return result

    smoke_test_start = BranchPythonOperator(
        task_id='smoke_test_start',
        provide_context=True,
        python_callable=_smoke_test_start,
        trigger_rule='none_failed_min_one_success',
        dag=dag,
    )

    smoke_test = KubernetesPodOperator(
      namespace='adtech-airflow',
      image="docker.prod.walmart.com/adtech-infra/dp-custom-image",
      cmds=["bash","-c"],
      arguments=[smoke_test_cmd],
      name="smoke_test",
      task_id="smoke-test",
      get_logs=True,
      is_delete_operator_pod=True,
      in_cluster=True,
      env_vars={
        'image_name': '{{ti.xcom_pull(task_ids="get-info", key="return_value")["test_image_name"]}}',
        'project_id': test_image_project,
        'zone': zone,
        'subnetwork': subnetwork,
        'no_external_ip': 'true',
        'service_account': 'svc-deploy-mgmt@wmt-customer-tech-adtech.iam.gserviceaccount.com',
        'cluster_name': cluster_name,
        'smoke_tests': config.get("smokeTest.tests"),
        'dp_version': '{{ti.xcom_pull(task_ids="get-info", key="return_value")["dp_version"]}}'
      },
      trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )


    # Below function will call the dag remotely
    def remote_dag_call(**context):
        data = json.loads(config.get("smokeTest.tests.dag"))
        if len(data) == 0:
            print("No dags declared in conf file, skipping the remote_dag_smoke_test")
        else:
            print("Running dags")
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            dag_run_id = f'remote-{timestamp}'
            print("dag_run_id:",dag_run_id)
            headers = { "Content-Type":"application/json", "Cache-Control": "no-cache" }
            # data = json.loads(config.get("smokeTest.tests.dag"))
            threads = []
            # create a queue for storing dag results
            result_queue = queue.Queue()
            params_default={'cluster_name': cluster_name,'region': region}
            with open('/etc/secrets/svc_adtech_infra_airflow', 'r') as pass_file:
              auth_password = pass_file.read().strip()
            endpoint_list=[]
            for dag in data:
                dag_id = dag['dagId']
                airflow_url = dag['airflowUrl']
                endpoint=airflow_url+"/api/v1/dags/"+dag_id+"/dagRuns"
                data = {'dag_run_id': dag_run_id, 'conf': params_default }
                print("dag_id:",dag_id)
                print("endpoint:",endpoint)
                print("data:",data)
                print("json_data:",json.dumps(data))
                # Sending dag trigger request
                response = requests.post(endpoint, headers=headers, data=json.dumps(data),  auth=('SVCAdtechAirflow', auth_password))
                response.raise_for_status()
                response_json = json.loads(response.content)
                print("dag_state:",response_json['state'])
                endpoint_list.append(f"{endpoint}/{dag_run_id}")
            context['ti'].xcom_push(key='endpoints', value=endpoint_list)
            print("All triggered Dags completed successfully")    

    remote_dag_smoke_test = PythonOperator(
        task_id='remote_dag_smoke_test',
        python_callable=remote_dag_call,
        provide_context=True,
        dag=dag,
    )

    # Below function will check the remote dags results
    def remote_dag_result_func(**context):
        data = json.loads(config.get("smokeTest.tests.dag"))
        if len(data) == 0:
            print("No dags declared in conf file, skipping the remote_dag_smoke_test")
            return True
        else:
            print("Running monitor dags")
            headers = { "Content-Type":"application/json", "Cache-Control": "no-cache" }
            endpoints = context['ti'].xcom_pull(key='endpoints')
            with open('/etc/secrets/svc_adtech_infra_airflow', 'r') as pass_file:
              auth_password = pass_file.read().strip()
            print("endpoints:",endpoints)
            for endpoint in endpoints:
                response = requests.get(endpoint, headers=headers, auth=('SVCAdtechAirflow', auth_password))
                response.raise_for_status()
                response_json = json.loads(response.content)
                state = response_json['state']
                print(f" state:{state}")
                if state == 'success' :
                    continue
                elif state == 'failed':
                    dag_id=endpoint.split("dags/")[1].split("/dagRuns")[0]
                    raise AirflowFailException(f"{dag_id} has failed permanently with no retries.")
                else:
                    return False
            return True

    remote_dag_result = PythonSensor(
        task_id="remote_dag_result",
        python_callable=remote_dag_result_func,
        mode='reschedule',
        poke_interval=10,
        dag=dag
    ) 

    skip_remote_and_smoke_test = DummyOperator(task_id='skip_remote_and_smoke_test', dag=dag)

    labels = KubernetesPodOperator(namespace='adtech-airflow',
      image="docker.prod.walmart.com/adtech-infra/common/utility-helper:latest",
      cmds=["/bin/bash","-c"],
      arguments=[labels_cmd],
      name="labels_cmd",
      task_id="labels",
      get_logs=True,
      is_delete_operator_pod=True,
      in_cluster=True,
      trigger_rule='none_failed_min_one_success',
      env_vars={
        'test_image_name': '{{ti.xcom_pull(task_ids="get-info", key="return_value")["test_image_name"]}}',
        'project_id': test_image_project,
        'owner': config.get("ownerLabel"),
        "jira_ticket": config.get("jiraTicket").lower()
      }
    )

    promote_image = DummyOperator(task_id='promote_image', dag=dag)
    promote_common_image = DummyOperator(task_id='promote_common_image', dag=dag)

    # promote_image = KubernetesPodOperator(
    #   namespace='adtech-airflow',
    #     image="docker.prod.walmart.com/adtech-infra/dp-custom-image",
    #     cmds=["bash","-c"],
    #     arguments=[promote_image_cmd],
    #     name="promote_image",
    #     task_id="promote-image",
    #     get_logs=True,
    #     is_delete_operator_pod=True,
    #     in_cluster=True,
    #     env_vars={
    #       'source_image': '{{ti.xcom_pull(task_ids="get-info", key="return_value")["test_image_name"]}}',
    #       'source_image_project': test_image_project,
    #       'dest_image': '{{ti.xcom_pull(task_ids="get-info", key="return_value")["image_name"]}}',
    #       'dest_image_project': image_project,
    #       'dest_image_family': image_family
    #     }
    # )

    get_info >> create_image >> branching >> [create_cluster, skip_create_cluster] >> smoke_test_start
    smoke_test_start >> smoke_test >> labels
    smoke_test_start >> remote_dag_smoke_test >> remote_dag_result >> labels
    smoke_test_start >> skip_remote_and_smoke_test >> labels
    labels >> [promote_image, promote_common_image] 

    # get_info >> create_image >> branching >> [create_cluster, skip_create_cluster] >> evaluate_conditions_task
    # evaluate_conditions_task >> [skip_all_tests, skip_smoke_test, skip_remote_dag, execute_all_tests]
    # skip_all_tests >> labels
    # skip_smoke_test >> remote_dag_smoke_test >> remote_dag_result >> labels
    # skip_remote_dag >> smoke_test >> labels
    # execute_all_tests >> [smoke_test, remote_dag_smoke_test, remote_dag_result] >> labels
    # labels >> promote_image
    

  return dag 

# Dynamically load dags from yaml files
files = glob.glob(os.path.dirname(os.path.realpath(__file__)) + "/conf/*.yaml")
for f in files:
    config = ImageConfig(f)
    dag_id = "ci_{}".format(config.get("dagId"))
    globals()[dag_id] = create_dag(dag_id, config)
