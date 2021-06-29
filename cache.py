import logging
import sys
import typing as t
from datetime import datetime, timedelta
from functools import wraps

log = logging.getLogger(__name__)

# 10 Megabytes
DEFAULT_CACHE_SIZE = 1_048_576_0
DEFAULT_CACHE_LIFETIME_SECONDS = 30


class CozyCache:
    def __init__(
            self,
            seconds: timedelta = timedelta(seconds=DEFAULT_CACHE_LIFETIME_SECONDS),
            size: int = DEFAULT_CACHE_SIZE,
    ):
        self.data = {}
        self.lifetime = seconds
        self.size = size

    @staticmethod
    def __make_collection_hashable(collection: t.Iterable) -> t.Iterable:
        if not collection:
            return ''

        if isinstance(collection, t.Set):
            return sorted(collection)

        if isinstance(collection, t.List):
            return collection

        # TODO: make something like recursive conversion to tuple of tuples
        if isinstance(collection, t.Dict):
            return sorted(collection)

        # TODO: make possible to receive ints, strings, returning them as [int]/[str] ?

        raise RuntimeError("Unsupported collection type: %s :  %s.", type(collection), collection)

    @classmethod
    def __make_key(
            cls,
            operation_name: str,
            collections,
    ) -> str:

        log.debug("Incoming collections: %s", collections)

        key = f"{operation_name}"
        for coll in collections:
            coll = cls.__make_collection_hashable(coll)
            key += str(coll)

        log.debug("Outgoing key: %s", key)
        return key

    def set_item(
            self,
            operation_name: str,
            item: t.Any,
            collections,
    ) -> None:
        key = self.__make_key(operation_name, collections)

        cache_size = sys.getsizeof(self.data)
        item_size = sys.getsizeof(item)

        if item_size >= self.size:
            log.warning("Item is too large: %s. Cache size: %s.", item_size, self.size)
            return

        if cache_size + item_size >= self.size:
            self.clean_old_records(item_size)

        self.data[key] = [item, datetime.utcnow() + self.lifetime, 0]

    def clean_old_records(self, needful_size: int) -> None:
        log.debug("Going to clean cache. Need to clean: %s.", needful_size)
        # structure: [['key1', item_size: int], ['key2', item_size: int], ...]
        irrelevant_items_first = [
            [k, sys.getsizeof(v[0])] for k, v in sorted(
                self.data.items(), key=lambda item: item[1][2]
            )
        ]

        cleared_size = 0
        for key, size in irrelevant_items_first:
            del self.data[key]
            cleared_size += size
            log.debug("Cleared %s bytes.", cleared_size)
            if cleared_size >= needful_size:
                break
        else:
            log.error(
                "Error: Cleared size is: %s. Nothing to clean anymore. "
                "But it`s still not enough space for item of size: %s. Cache size is: %s.",
                cleared_size,
                needful_size,
                self.size,
            )
            raise RuntimeError("Cache cleanup error.")

        log.debug("Cleared %s bytes totally. Was in need of: %s", cleared_size, needful_size)

    def get_item(
            self,
            operation_name: str,
            collections: t.Iterable[t.Any]
    ) -> t.Optional[t.Any]:
        key = self.__make_key(operation_name, collections)

        try:
            value = self.data[key]
        except KeyError:
            return None

        if datetime.utcnow() >= value[1]:
            del self.data[key]
            return None

        value[2] += 1
        return value[0]


def timed_sized_cache_async(
        seconds: t.Union[int, timedelta] = timedelta(seconds=DEFAULT_CACHE_LIFETIME_SECONDS),
        size: int = DEFAULT_CACHE_SIZE,
):
    if isinstance(seconds, int):
        seconds = timedelta(seconds=seconds)

    async def cache_wrapper(func):
        log.debug("Creating in-memory cache for function %s, size: %s, lifetime: %s.", func.__name__, size, seconds)
        cache = CozyCache(seconds=seconds, size=size)

        @wraps(func)
        def wrapped_func(*args, **kwargs):
            op_name = func.__name__
            collections = [val for name, val in kwargs.items()]

            cached_item = cache.get_item(op_name, collections)
            if cached_item is not None:
                log.debug('Cache hit: Incoming collections: %s', collections)
                return cached_item

            result = await func(*args, **kwargs)

            log.debug('Cache miss. Will cache result. Incoming collections: %s', collections)
            cache.set_item(op_name, result, collections)

            return result

        return wrapped_func

    return cache_wrapper
