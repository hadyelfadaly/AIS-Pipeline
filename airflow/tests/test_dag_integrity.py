import pytest
from airflow.models import DagBag

@pytest.fixture(scope="session")
def dagbag():
    return DagBag(dag_folder="dags/") #Loads all DAGs from the 'dags/' directory

def test_no_import_errors(dagbag):
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"

def test_expected_dags_loaded(dagbag):

    expected_dags = {"ingest_reference_data", "activity_backfill", "activity_weekly"}

    assert expected_dags.issubset(set(dagbag.dag_ids))

def test_dag_conventions(dagbag):
    
    for dag_id, dag in dagbag.dags.items():

        #Ensure catchup is False for all except backfill
        if dag_id != "activity_backfill":
            assert dag.catchup is False, f"{dag_id} should have catchup=False"
        
        #Ensure tags exist (good practice)
        assert len(dag.tags) > 0, f"{dag_id} is missing tags"