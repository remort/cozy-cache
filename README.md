# cozy-cache
Timed, sized, with hit counts cache class and decorator.

# example

    from cache import CozyCache, timed_sized_cache

    @timed_sized_cache()
    def fn(col1):
        return [x*2 for x in col1]

    fn(col1=[4,0,9]) #  returns uncached result
    fn(col1=[4,0,9]) #  returns cached result second time

    fn(col1={2,3,6}) #  new collection. will return uncached result first time
    fn(col1={2,3,6}) #  returns cached result second time
