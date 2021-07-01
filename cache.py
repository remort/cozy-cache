import logging
import sys
import typing as t
from collections import namedtuple
from datetime import datetime, timedelta
from functools import wraps

DecoratedFunc = t.Callable[..., t.Any]
CollectionItem = t.TypeVar("CollectionItem")

log = logging.getLogger(__name__)

# 10 Megabytes
DEFAULT_CACHE_SIZE = 1_048_576_0
DEFAULT_CACHE_LIFETIME_SECONDS = 30

ItemStructure = namedtuple("cache_item", ("value", "expiration_time", "size", "hit_count"))


class TimedSizedCache:
    def __init__(
            self,
            seconds: timedelta = timedelta(seconds=DEFAULT_CACHE_LIFETIME_SECONDS),
            size: int = DEFAULT_CACHE_SIZE,
    ):
        self.data: t.Dict[str, ItemStructure] = {}
        self.lifetime = seconds
        self.size = size

    @classmethod
    def __getsizeof(cls, obj: t.Any) -> int:
        """
        Deliberate implementation, compiled based on this thread:
        https://stackoverflow.com/questions/449560/how-do-i-determine-the-size-of-an-object-in-python
        """
        size = sys.getsizeof(obj)
        if isinstance(obj, dict):
            return size + sum(map(cls.__getsizeof, obj.keys())) + sum(map(cls.__getsizeof, obj.values()))
        if isinstance(obj, (list, tuple, set, frozenset)):
            return size + sum(map(cls.__getsizeof, obj))
        # For dataclasses
        if hasattr(obj, '__dict__'):
            return size + sum(map(cls.__getsizeof, vars(obj)))
        return size

    @staticmethod
    def __make_collection_ordered(collection: t.Iterable[CollectionItem]) -> t.List[CollectionItem]:
        # https://github.com/python/mypy/issues/3060
        if isinstance(collection, (t.Set, t.Tuple, t.List)):  # type: ignore
            # https://github.com/python/typing/issues/760
            return sorted(collection)  # type: ignore

        raise RuntimeError(f"Unsupported collection type: {type(collection)} : {collection}.")

    @classmethod
    def __make_key(
            cls,
            operation_name: str,
            collections: t.Tuple[t.Iterable[t.Any], ...],
    ) -> str:

        log.debug("Incoming collections to form a key: %s", collections)

        key = operation_name
        for coll in collections:
            if not coll:
                continue

            coll = cls.__make_collection_ordered(coll)
            key += "".join(str(i) if not isinstance(i, str) else i for i in coll)

        log.debug("Collections turned into this key: %s", key)
        return key

    def set_item(
            self,
            operation_name: str,
            item: t.Any,
            collections: t.Tuple[t.Iterable[t.Any], ...],
    ) -> None:
        key = self.__make_key(operation_name, collections)

        cache_size = sum([item.size for item in self.data.values()])
        item_size = self.__getsizeof(item)

        if item_size >= self.size:
            log.warning("Item is too large: %s. Cache size: %s.", item_size, self.size)
            return

        if cache_size + item_size >= self.size:
            self.clean_old_records(item_size)

        self.data[key] = ItemStructure(
            value=item,
            expiration_time=datetime.utcnow() + self.lifetime,
            size=item_size,
            hit_count=0,
        )

    def clean_old_records(self, needful_size: int) -> None:
        log.debug("Going to clean cache. Need to clean: %s.", needful_size)

        # structure: [['key1', item_size: int], ['key2', item_size: int], ...]
        irrelevant_items_first: t.List[t.Tuple[str, int]] = [
            (k, v.size) for k, v in sorted(
                self.data.items(), key=lambda item: item[1].hit_count
            )
        ]

        cleared_size = 0

        key: str
        size: int
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
            collections: t.Tuple[t.Iterable[t.Any], ...],
    ) -> t.Any:
        key = self.__make_key(operation_name, collections)

        try:
            value = self.data[key]
        except KeyError:
            return None

        if datetime.utcnow() >= value.expiration_time:
            del self.data[key]
            return None

        value._replace(hit_count=value.hit_count+1)
        return value.value


def timed_sized_cache(
        seconds: t.Union[int, timedelta] = timedelta(seconds=DEFAULT_CACHE_LIFETIME_SECONDS),
        size: int = DEFAULT_CACHE_SIZE,
) -> t.Callable[[DecoratedFunc], t.Callable[..., t.Any]]:
    if isinstance(seconds, int):
        seconds = timedelta(seconds=seconds)

    def cache_wrapper(func: DecoratedFunc) -> t.Callable[..., t.Any]:
        log.debug("Creating in-memory cache for function %s, size: %s, lifetime: %s.", func.__name__, size, seconds)
        cache = TimedSizedCache(seconds=t.cast(timedelta, seconds), size=size)

        @wraps(func)
        def wrapped_func(*args: t.Any, **kwargs: t.Any) -> t.Any:
            op_name = func.__name__
            collections = [val for name, val in kwargs.items()]

            cached_item = cache.get_item(op_name, tuple(collections))
            if cached_item is not None:
                log.debug('Cache hit: Incoming collections: %s', collections)
                return cached_item

            result = func(*args, **kwargs)

            log.debug("Cache miss. Will cache result. Incoming collections: %s", collections)
            cache.set_item(op_name, result, tuple(collections))

            return result

        return wrapped_func

    return cache_wrapper
