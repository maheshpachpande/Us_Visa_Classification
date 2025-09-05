import sys
from us_visa.logger import logging
from us_visa.exception import USvisaException
from us_visa.pipeline.stage_01_data_ingestion_pipe import DataIngestionPipeline
import matplotlib
matplotlib.use('Agg')  # Use non-GUI backend to avoid tkinter errors



STAGE_NAME = "Data Ingestion stage"


try:
    logging.info(f">>>>>> stage {STAGE_NAME} started <<<<<<")
    obj = DataIngestionPipeline()
    obj.run()
    logging.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
except Exception as e:
    logging.exception(e)
    raise USvisaException(e, sys)
