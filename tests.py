from datetime import datetime, timedelta
from time import sleep

import pytest

from cache import timed_sized_cache


@pytest.fixture()
def function_min_duration() -> timedelta:
    return timedelta(seconds=0.2)


@pytest.fixture(name="fun")
def sample_func(function_min_duration):
    def fn(col):
        sleep(function_min_duration.microseconds/1_000_000)
        return [x * x for x in col if x % 2 == 0]
    return fn


@pytest.fixture()
def collection_one():
    return [x for x in range(0, 100000)]


@pytest.fixture()
def collection_two():
    return [x for x in range(0, 100)]


@pytest.fixture()
def call_and_measure_time():
    def wrap(fn, collection) -> int:
        d1 = datetime.utcnow()
        fn(col=collection)
        d2 = datetime.utcnow()
        duration = (d2 - d1).microseconds
        return duration

    return wrap


def test_time_cache(fun, collection_one, call_and_measure_time):
    fun = timed_sized_cache(seconds=1)(fun)

    def runner():
        first_duration = 0
        prev_duration = 0
        for run in (range(0, 5)):
            duration = call_and_measure_time(fun, collection_one)

            # first function call. just save duration. no other durations to compare with yet.
            if run == 0:
                first_duration = duration
                continue
            # second function call. result should be taken from cache. duration must be much less than previous one.
            if run == 1:
                pass
                assert first_duration / duration > 5
            # a few other calls in between 1 second. durations are short and similar. but much less than the first one.
            if run > 1:
                assert prev_duration / duration < 2
                assert first_duration / duration > 5

            prev_duration = duration

    runner()
    sleep(1)
    runner()


def test_call_not_in_cache(fun, collection_one, collection_two, call_and_measure_time):
    fun = timed_sized_cache(seconds=1)(fun)

    first_duration_f1 = 0
    first_duration_f2 = 0
    prev_duration_f1 = 0
    prev_duration_f2 = 0
    duration_f2 = 0

    for run in (range(0, 5)):
        duration_f1 = call_and_measure_time(fun, collection_one)

        # starting from second run, call function with second argument every run.
        if run > 0:
            duration_f2 = call_and_measure_time(fun, collection_two)

            prev_duration_f2 = duration_f2

        # first run. function call with first argument called first time. save its initial duration.
        if run == 0:
            first_duration_f1 = duration_f1

        # second run. function call with second argument called first time. save it's duration.
        if run == 1:
            first_duration_f2 = duration_f2
            # assert that result of first function call with first argument was not cached.
            assert prev_duration_f1 / duration_f1 > 5

        # third run. function call with first argument is cached, call with second argument is not cached yet.
        if run == 2:
            assert prev_duration_f1 / duration_f1 < 2
            assert first_duration_f1 / duration_f1 > 5

            # function call with second argument. long duration, call with non-cached result.
            assert first_duration_f2 / duration_f2 > 5

        # all function calls with both arguments are cached by now. their calls durations are short.
        if run == 3:
            assert prev_duration_f1 / duration_f1 < 2
            assert first_duration_f1 / duration_f1 > 5

            assert prev_duration_f2 / duration_f2 < 2
            assert first_duration_f2 / duration_f2 > 5

        prev_duration_f1 = duration_f1


def test_size(fun, collection_one, collection_two, call_and_measure_time, function_min_duration):
    fun = timed_sized_cache(seconds=10, size=5000)(fun)

    for x in range(0, 10):

        # This call is never being cached since it's result is too big.
        duration_f1 = call_and_measure_time(fun, collection_one)

        # This call's result is small and can be cached.
        duration_f2 = call_and_measure_time(fun, collection_two)

        assert duration_f1 > function_min_duration.microseconds

        if x == 0:
            assert duration_f2 > function_min_duration.microseconds
        else:
            assert duration_f2 < function_min_duration.microseconds
