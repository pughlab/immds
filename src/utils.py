import logging
import os

def get_logger(app_name, file_name):
    # create logger with app_name
    if not dir_exists(os.path.os.path.dirname(file_name)):
        raise Exception("cannot create a log file")
    logger = logging.getLogger(app_name)
    logger.setLevel(logging.DEBUG)

    # create file handler which logs even debug messages
    fh = logging.FileHandler(file_name)
    fh.setLevel(logging.DEBUG)

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

def dir_exists(dir_name):
    if not os.path.exists(dir_name):
        try:
            os.makedirs(dir_name)
        except:
            return False
    return True