import logging

logger = logging.getLogger('WorkflowManager')

class WorkflowManagerLogger:
    def __init__(self):
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        self.LOGGER = logger