import logging

file_handler = logging.FileHandler("app.log")
stream_handler = logging.StreamHandler()

format = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s'
level = logging.DEBUG
handlers = [file_handler, stream_handler]

logging.basicConfig(level=level, format=format, handlers=handlers)

logger = logging.getLogger(__name__)

