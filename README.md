# timed-sized-cache
Cache class and decorator, configurable by lifetime and maximum size, with hit counts.

# example

    from cache import timed_sized_cache

    @timed_sized_cache()
    def fn(col1):
        return [x*2 for x in col1]

    fn(col1=[4,0,9]) #  returns uncached result
    fn(col1=[4,0,9]) #  returns cached result second time

    fn(col1={2,3,6}) #  new collection. will return uncached result first time
    fn(col1={2,3,6}) #  returns cached result second time

# time and size limits

    @timed_sized_cache(seconds=10, size=500)
    def fn(col1):
        return [x*2 for x in col1]

    fn(col1=[x for x in range(0,15)])
    > Item is too large: 3700. Cache size: 500.
    > [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28]  # calculated result is not stored in the cache due to item size overlimit.

    fn(col1=[x for x in range(0,5)])
    > [0, 2, 4, 6, 8]  # result is calculated for the first time. stored to cache.

    # 5 seconds later:
    fn(col1=[x for x in range(0,5)])
    > [0, 2, 4, 6, 8]  # result is taken from cache.

    # 5 seconds later:
    fn(col1=[x for x in range(0,5)])
    > [0, 2, 4, 6, 8]  # result is calculated again since cache item lifetime is out.
