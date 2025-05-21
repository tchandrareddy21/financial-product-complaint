from datetime import datetime
from airflow.decorators import dag, task
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

# Initialize training pipeline before the DAG definition
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
        # Reconstruct the artifact object from dictionary
        ingestion_artifact = DataIngestionArtifact(
            feature_store_file_path=data_ingestion_artifact['feature_store_file_path'],
            metadata_file_path=data_ingestion_artifact['metadata_file_path'],
            download_dir=data_ingestion_artifact['download_dir']
        )
        
        data_validation_artifact = training_pipeline.start_data_validation(
            data_ingestion_artifact=ingestion_artifact
        )
        
        return (
            data_validation_artifact._asdict()
            if hasattr(data_validation_artifact, "_asdict")
            else dict(data_validation_artifact)
        )
    
    @task()
    def data_transformation(data_validation_artifact: dict):
        """
        Task to transform the validated data
        """
        # Reconstruct validation artifact
        validation_artifact = DataValidationArtifact(
            accepted_file_path=data_validation_artifact['accepted_file_path'],
            rejected_dir=data_validation_artifact['rejected_dir']
        )
        
        data_transformation_artifact = training_pipeline.start_data_transformation(
            data_validation_artifact=validation_artifact
        )
        
        return (
            data_transformation_artifact._asdict()
            if hasattr(data_transformation_artifact, "_asdict")
            else dict(data_transformation_artifact)
        )
    
    @task()
    def model_trainer(data_transformation_artifact: dict):
        """
        Task to train the model
        """
        # Reconstruct transformation artifact
        transformation_artifact = DataTransformationArtifact(
            transformed_train_file_path=data_transformation_artifact['transformed_train_file_path'],
            exported_pipeline_file_path=data_transformation_artifact['exported_pipeline_file_path'],
            transformed_test_file_path=data_transformation_artifact['transformed_test_file_path']
        )
        
        model_trainer_artifact = training_pipeline.start_model_trainer(
            data_transformation_artifact=transformation_artifact
        )
        
        return model_trainer_artifact._asdict()
    
    @task()
    def model_evaluation(data_validation_artifact: dict, model_trainer_artifact: dict):
        """
        Task to evaluate the trained model
        """
        # Reconstruct validation artifact
        validation_artifact = DataValidationArtifact(
            accepted_file_path=data_validation_artifact['accepted_file_path'],
            rejected_dir=data_validation_artifact['rejected_dir']
        )
        
        # Reconstruct model trainer artifact using custom constructor
        trainer_artifact = ModelTrainerArtifact.construct_object(**model_trainer_artifact)
        
        model_evaluation_artifact = training_pipeline.start_model_evaluation(
            data_validation_artifact=validation_artifact,
            model_trainer_artifact=trainer_artifact
        )
        
        return model_evaluation_artifact.to_dict()
    
    @task()
    def push_model(model_evaluation_artifact: dict, model_trainer_artifact: dict):
        """
        Task to push the model if accepted
        """
        # Reconstruct evaluation artifact
        evaluation_artifact = ModelEvaluationArtifact(
            model_accepted=model_evaluation_artifact['model_accepted'],
            changed_accuracy=model_evaluation_artifact['changed_accuracy'],
            trained_model_path=model_evaluation_artifact['trained_model_path'],
            best_model_path=model_evaluation_artifact['best_model_path'],
            active=model_evaluation_artifact['active']
        )
        
        # Reconstruct model trainer artifact
        trainer_artifact = ModelTrainerArtifact.construct_object(**model_trainer_artifact)
        
        if evaluation_artifact.model_accepted:
            model_pusher_artifact = training_pipeline.start_model_pusher(
                model_trainer_artifact=trainer_artifact
            )
            print(f'Model pusher artifact: {model_pusher_artifact}')
            return {"model_pushed": True}
        else:
            print("Trained model rejected.")
            return {"model_pushed": False}
    
    # Set up the workflow
    ingestion_output = data_ingestion()
    validation_output = data_validation(ingestion_output)
    transformation_output = data_transformation(validation_output)
    trainer_output = model_trainer(transformation_output)
    evaluation_output = model_evaluation(validation_output, trainer_output)
    push_model(evaluation_output, trainer_output)

# Create the DAG
finance_pipeline()
