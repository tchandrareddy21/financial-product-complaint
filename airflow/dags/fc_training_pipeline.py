from datetime import datetime
from airflow.sdk import DAG
from textwrap import dedent
from airflow.providers.standard.operators.python import PythonOperator

training_pipeline=None

with DAG(
    dag_id="finance_product_complaint",
    default_args={'retries': 2},
    description="Machine Learning PySpark Project",
    schedule="@daily",
    start_date=datetime(2025, 4, 10),
    catchup=False,
    tags=['PySpark', 'finance']
) as dag:

    from finance.pipeline.training import TrainingPipeline
    from finance.config.pipeline.training import FinanceConfig

    training_pipeline = TrainingPipeline(FinanceConfig())
    

    def data_ingestion(**kwargs):
        from finance.entity.artifact_entity import (DataIngestionArtifact,
                                            DataValidationArtifact,
                                            DataTransformationArtifact,
                                            ModelTrainerArtifact,
                                            PartialModelTrainerRefArtifact,
                                            PartialModelTrainerMetricArtifact,
                                            ModelEvaluationArtifact,
                                            ModelPusherArtifact)
        ti = kwargs['ti']
        data_ingestion_artifact = training_pipeline.start_data_ingestion()
        print(data_ingestion_artifact)
        ti.xcom_push('data_ingestion_artifact', data_ingestion_artifact)
    

    def data_validation(**kwargs):
        from finance.entity.artifact_entity import (DataIngestionArtifact,
                                            DataValidationArtifact,
                                            DataTransformationArtifact,
                                            ModelTrainerArtifact,
                                            PartialModelTrainerRefArtifact,
                                            PartialModelTrainerMetricArtifact,
                                            ModelEvaluationArtifact,
                                            ModelPusherArtifact)
        ti  = kwargs['ti']
        data_ingestion_artifact = ti.xcom_pull(task_ids="data_ingestion",key="data_ingestion_artifact")
        data_ingestion_artifact=DataIngestionArtifact(*(data_ingestion_artifact))
        data_validation_artifact=training_pipeline.start_data_validation(data_ingestion_artifact=data_ingestion_artifact)
        ti.xcom_push('data_validation_artifact', data_validation_artifact)
    


    def data_transformation(**kwargs):
        from finance.entity.artifact_entity import (DataIngestionArtifact,
                                            DataValidationArtifact,
                                            DataTransformationArtifact,
                                            ModelTrainerArtifact,
                                            PartialModelTrainerRefArtifact,
                                            PartialModelTrainerMetricArtifact,
                                            ModelEvaluationArtifact,
                                            ModelPusherArtifact)
        ti  = kwargs['ti']

        data_ingestion_artifact = ti.xcom_pull(task_ids="data_ingestion",key="data_ingestion_artifact")
        data_ingestion_artifact=DataIngestionArtifact(*(data_ingestion_artifact))

        data_validation_artifact = ti.xcom_pull(task_ids="data_validation",key="data_validation_artifact")
        data_validation_artifact=DataValidationArtifact(*(data_validation_artifact))
        data_transformation_artifact=training_pipeline.start_data_transformation(
        data_validation_artifact=data_validation_artifact
        )
        ti.xcom_push('data_transformation_artifact', data_transformation_artifact)


    def model_trainer(**kwargs):
        from finance.entity.artifact_entity import (DataIngestionArtifact,
                                            DataValidationArtifact,
                                            DataTransformationArtifact,
                                            ModelTrainerArtifact,
                                            PartialModelTrainerRefArtifact,
                                            PartialModelTrainerMetricArtifact,
                                            ModelEvaluationArtifact,
                                            ModelPusherArtifact)
        ti  = kwargs['ti']

        data_transformation_artifact = ti.xcom_pull(task_ids="data_transformation",key="data_transformation_artifact")
        data_transformation_artifact=DataTransformationArtifact(*(data_transformation_artifact))

        model_trainer_artifact=training_pipeline.start_model_trainer(data_transformation_artifact=data_transformation_artifact)

        ti.xcom_push('model_trainer_artifact', model_trainer_artifact._asdict())


    def model_evaluation(**kwargs):
        from finance.entity.artifact_entity import (DataIngestionArtifact,
                                            DataValidationArtifact,
                                            DataTransformationArtifact,
                                            ModelTrainerArtifact,
                                            PartialModelTrainerRefArtifact,
                                            PartialModelTrainerMetricArtifact,
                                            ModelEvaluationArtifact,
                                            ModelPusherArtifact)
        ti  = kwargs['ti']
        data_ingestion_artifact = ti.xcom_pull(task_ids="data_ingestion",key="data_ingestion_artifact")
        data_ingestion_artifact=DataIngestionArtifact(*(data_ingestion_artifact))

        data_validation_artifact = ti.xcom_pull(task_ids="data_validation",key="data_validation_artifact")
        data_validation_artifact=DataValidationArtifact(*(data_validation_artifact))

        model_trainer_artifact = ti.xcom_pull(task_ids="model_trainer",key="model_trainer_artifact")
        print(model_trainer_artifact)
        model_trainer_artifact=ModelTrainerArtifact.construct_object(**model_trainer_artifact)

        model_evaluation_artifact = training_pipeline.start_model_evaluation(data_validation_artifact=data_validation_artifact,
                                                                    model_trainer_artifact=model_trainer_artifact)

    
        ti.xcom_push('model_evaluation_artifact', model_evaluation_artifact.to_dict())


    def push_model(**kwargs):
        from finance.entity.artifact_entity import (DataIngestionArtifact,
                                            DataValidationArtifact,
                                            DataTransformationArtifact,
                                            ModelTrainerArtifact,
                                            PartialModelTrainerRefArtifact,
                                            PartialModelTrainerMetricArtifact,
                                            ModelEvaluationArtifact,
                                            ModelPusherArtifact)
        ti  = kwargs['ti']
        model_evaluation_artifact = ti.xcom_pull(task_ids="model_evaluation",key="model_evaluation_artifact")
        model_evaluation_artifact=ModelEvaluationArtifact(*(model_evaluation_artifact))
        model_trainer_artifact = ti.xcom_pull(task_ids="model_trainer",key="model_trainer_artifact")
        model_trainer_artifact=ModelTrainerArtifact.construct_object(**model_trainer_artifact)

        if model_evaluation_artifact.model_accepted:
            model_pusher_artifact = training_pipeline.start_model_pusher(model_trainer_artifact=model_trainer_artifact)
            print(f'Model pusher artifact: {model_pusher_artifact}')
        else:
            print("Trained model rejected.")
            print("Trained model rejected.")
        print("Training pipeline completed")
    

    data_ingestion = PythonOperator(
        task_id='data_ingestion',
        python_callable=data_ingestion,
    )
    data_ingestion.doc_md = dedent(
        """\
    #### Extract task
    A simple Extract task to get data ready for the rest of the data pipeline.
    In this case, getting data is simulated by reading from a hardcoded JSON string.
    This data is then put into xcom, so that it can be processed by the next task.
    """
    )

    data_validation = PythonOperator(
        task_id="data_validation",
        python_callable=data_validation

    )

    data_transformation = PythonOperator(
        task_id ="data_transformation",
        python_callable=data_transformation
    )

    model_trainer = PythonOperator(
        task_id="model_trainer", 
        python_callable=model_trainer

    )

    model_evaluation = PythonOperator(
        task_id="model_evaluation", python_callable=model_evaluation
    )   

    push_model  =PythonOperator(
            task_id="push_model",
            python_callable=push_model

    )

    data_ingestion >> data_validation >> data_transformation >> model_trainer >> model_evaluation >> push_model
