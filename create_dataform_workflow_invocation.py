from datetime import datetime

from google.cloud.dataform_v1beta1 import WorkflowInvocation
from airflow.operators.dummy import DummyOperator

from airflow import models
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.dataform import (
    DataformCancelWorkflowInvocationOperator,
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
    DataformGetCompilationResultOperator,
    DataformGetWorkflowInvocationOperator,
)

DAG_ID = "dataform"
PROJECT_ID = "YOUR-PROJECT"
REPOSITORY_ID = "quickstart-repository"
REGION = "us-central1"
GIT_COMMITISH = "main"

with models.DAG(
    DAG_ID,
    schedule_interval='@once',  # Override to match your needs
    start_date=datetime(2023, 6, 12),
    catchup=False,  # Override to match your needs
    tags=['dataform'],
) as dag:

    dummy1 = DummyOperator(
        task_id="dummy1"
    )

    dummy2 = DummyOperator(
        task_id="dummy2"
    )

    dummy3 = DummyOperator(
        task_id="dummy3"
    )

    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": GIT_COMMITISH,
        },
    )

    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
         workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}"
           , "invocation_config": { "included_tags": ["country"], "transitive_dependencies_included": True }
        },
    )

[dummy1, dummy2] >> dummy3 >> create_compilation_result >> create_workflow_invocation
