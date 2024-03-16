import logging
from logging.handlers import RotatingFileHandler
import os

class LoggerFactory:
    def __init__(self, logFileName :str):
        self.base_filename = f'{logFileName}.log'
        log_folder = os.path.join(os.path.dirname(__file__), 'Logs')  # Folder named 'logs' next to the script
        os.makedirs(log_folder, exist_ok=True)  # Create the 'logs' folder if it doesn't exist
        log_file_path = os.path.join(log_folder, self.base_filename)

        self.file_handler = self._create_file_handler(log_file_path)

    def _create_file_handler(self, log_file_path):
        log_file_path = self._get_unique_log_filename(log_file_path)

        file_handler = RotatingFileHandler(log_file_path, maxBytes=1024, backupCount=0, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)  # Set the logging level to DEBUG

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        return file_handler

    def _get_unique_log_filename(self, base_filename):
        if not os.path.exists(base_filename):
            return base_filename

        filename, file_extension = os.path.splitext(base_filename)
        counter = 1
        while True:
            new_filename = f"{filename}-{counter}{file_extension}"
            if not os.path.exists(new_filename):
                return new_filename
            counter += 1

    def getLogger(self, logger_name) -> logging.Logger:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)  # Set the logging level to DEBUG

        logger.addHandler(self.file_handler)
        logger.debug(f"{logger_name} logger constructed")
        return logger
    
if __name__ == "__main__":
    loggerFactory = LoggerFactory()
    logger1 = loggerFactory.getLogger("LoggerFactory")
    logger1.debug("testing1234")
    logger2 = loggerFactory.getLogger("Factory2")
    logger2.debug("4321testing")