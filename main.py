import os
import argparse
from finance.exception import FinancialException
from finance.pipeline import TrainingPipeline
from finance.logger import logger
from finance.config.pipeline.training import FinanceConfig
import sys


def start_training(start=False):
    try:
        if not start:
            return None
        print("Training Running")
        TrainingPipeline(FinanceConfig()).start()
        
    except Exception as e:
        raise FinancialException(e, sys)


# def start_prediction(start=False):
#     try:
#         if not start:
#             return None
#         print("Prediction started")
#         PredictionPipeline().start_batch_prediction()
        
#     except Exception as e:
#         raise FinancialException(e, sys)


def main(training_status):
    try:

        start_training(start=training_status)
        # start_prediction(start=prediction_status)
    except Exception as e:
        raise FinancialException(e, sys)


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--t", default=0, type=int, help="If provided true training will be done else not")
        # parser.add_argument("--p", default=0, type=int, help="If provided prediction will be done else not")

        args = parser.parse_args()

        # main(training_status=args.t, prediction_status=args.p)
        main(training_status=args.t)
    except Exception as e:
        print(e)
        pass
        logger.exception(FinancialException(e, sys))
