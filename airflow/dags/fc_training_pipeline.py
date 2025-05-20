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
    
    @task()
    def data_transformation(data_ingestion_artifact: dict, data_validation_artifact: dict):
        """
        Task to transform the data using the ingestion and validation artifacts
        """
        data_transformation_artifact = training_pipeline.start_data_transformation(
            *(data_ingestion_artifact), *(data_validation_artifact)
        )
        return (
            data_transformation_artifact._asdict()
            if hasattr(data_transformation_artifact, "_asdict")
            else dict(data_transformation_artifact)
        )
    
    @task()
    def model_trainer(data_transformation_artifact: dict):
        """
        Task to train the model using the transformation artifact
        """
        model_trainer_artifact = training_pipeline.start_model_trainer(
            *(data_transformation_artifact)
        )
        return (
            model_trainer_artifact._asdict()
        )
    
    @task()
    def model_evaluation(data_ingestion_artifact: dict, data_validation_artifact: dict, model_trainer_artifact: dict):
        """
        Task to evaluate the model using the trainer artifact
        """
        ingestion_artifact = DataIngestionArtifact(*data_ingestion_artifact)
        validation_artifact = DataValidationArtifact(*data_validation_artifact)
        trainer_artifact = ModelTrainerArtifact.construct_object(**model_trainer_artifact)
        evaluation_artifact = training_pipeline.start_model_evaluation(
            data_validation_artifact=validation_artifact,
            model_trainer_artifact=trainer_artifact
        )
        return evaluation_artifact.to_dict()
    
    @task
    def push_model(model_evaluation_artifact: dict, model_trainer_artifact: dict):
        evaluation_artifact = ModelEvaluationArtifact(*model_evaluation_artifact)
        trainer_artifact = ModelTrainerArtifact.construct_object(**model_trainer_artifact)
        if evaluation_artifact.model_accepted:
            pusher_artifact = training_pipeline.start_model_pusher(trainer_artifact)
            print(f'Model pusher artifact: {pusher_artifact}')
        else:
            print("Trained model rejected.")
        print("Training pipeline completed.")



    # Define DAG dependencies using chaining
    ingestion = data_ingestion()
    validation = data_validation(ingestion)
    transformation = data_transformation(ingestion, validation)
    trainer = model_trainer(transformation)
    evaluation = model_evaluation(ingestion, validation, trainer)
    push_model(evaluation, trainer)


finance_pipeline()
