import os

PIPELINE_NAME = "finance-product-complaint"
PIPELINE_ARTIFACT_DIR = os.path.join(os.getcwd(), "finance-artifact")

from finance.constants.training_pipeline_constants.data_ingestion_constants import *
from finance.constants.training_pipeline_constants.data_validation_constants import *
from finance.constants.training_pipeline_constants.data_transformation_constants import *
from finance.constants.training_pipeline_constants.model_trainer_constants import *
from finance.constants.training_pipeline_constants.model_evaluation_constants import *
