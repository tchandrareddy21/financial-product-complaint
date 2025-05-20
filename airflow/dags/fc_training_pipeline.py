from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from finance.pipeline.training import TrainingPipeline
from finance.config.pipeline.training import FinanceConfig
from finance.entity.artifact_entity import (DataIngestionArtifact,
                                            DataValidationArtifact,
                                            DataTransformationArtifact,
                                            ModelTrainerArtifact,
                                            PartialModelTrainerRefArtifact,
                                            PartialModelTrainerMetricArtifact,
                                            ModelEvaluationArtifact,
                                            ModelPusherArtifact)
training_pipeline = TrainingPipeline(FinanceConfig())

@dag(
    dag_id="finance_product_complaint",
    default_args={'retries': 2},
    description="Machine Learning PySpark Project",
    schedule="@daily",
    start_date=datetime(2025, 4, 10),
    catchup=False,
    tags=['PySpark', 'finance']
)
    
def finance_pipeline():

    @task()
    def data_ingestion():
        """
        Task to start data ingestion and return the artifact as a dict
        """
        data_ingestion_artifact = training_pipeline.start_data_ingestion()
        return data_ingestion_artifact._asdict() if hasattr(data_ingestion_artifact, '_asdict') else dict(data_ingestion_artifact)
    
    @task()
    def data_validation(data_ingestion_artifact: dict):
        """
        Task to validate the data using the ingestion artifact
        """
        data_validation_artifact = training_pipeline.start_data_validation(*(data_ingestion_artifact))
        return (
            data_validation_artifact._asdict()
            if hasattr(data_validation_artifact, "_asdict")
            else dict(data_validation_artifact)
        )


    data_ingestion_artifact = data_ingestion()
    data_validation_artifact = data_validation(data_ingestion_artifact)

finance_pipeline()