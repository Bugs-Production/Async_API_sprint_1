import logging

from .connection_backoff import backoff

logger = logging.getLogger(__name__)


@backoff()
def wait_for_db(db, message):
    if db.ping():
        logger.info(message)
