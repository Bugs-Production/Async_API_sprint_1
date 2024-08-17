import logging
from time import sleep

import elastic_transport
from redis.exceptions import ConnectionError as RedisConnectionError

logger = logging.getLogger(__name__)

BACKOFF_ITERATIONS_COUNT = 15


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    :param start_sleep_time: начальное время ожидания
    :param factor: множитель увеличения времени ожидания
    :param border_sleep_time: максимальное время ожидания
    :return: время ожидания в секундах
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            iteration = 0
            curr_sleep_time = start_sleep_time
            while iteration < BACKOFF_ITERATIONS_COUNT:
                logger.info(f"iteration {iteration}")
                try:
                    return func(*args, **kwargs)
                except (elastic_transport.ConnectionError, RedisConnectionError) as e:
                    logger.error(e)
                    if curr_sleep_time < border_sleep_time:
                        curr_sleep_time = start_sleep_time * factor**iteration

                    if curr_sleep_time > border_sleep_time:
                        curr_sleep_time = border_sleep_time

                    sleep(curr_sleep_time)

                    iteration += 1

        return wrapper

    return decorator
