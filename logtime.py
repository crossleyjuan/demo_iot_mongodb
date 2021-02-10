from datetime import datetime
import logging

logger = logging.getLogger(__name__)
console = logging.StreamHandler()

logger.setLevel(logging.DEBUG)
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)

def log_time():
    def log(method):
        # 'wrap' this puppy up if needed
        def wrapped(*args, **kwargs):
            # start timing
            ts = datetime.now()
            method(*args, **kwargs)
            # stop timing
            te = datetime.now()
            logger.debug('%r %s' % \
              (method.__name__, te-ts))
        return wrapped
    return log


