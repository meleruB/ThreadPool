import os

from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool

webserver = Flask(__name__)
webserver.tasks_runner = ThreadPool()

# webserver.task_runner.start()

webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

webserver.job_counter = 1

import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

log_file = 'logs/webservice.log'
handler = RotatingFileHandler(log_file, maxBytes=1024*1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)

from app import routes
