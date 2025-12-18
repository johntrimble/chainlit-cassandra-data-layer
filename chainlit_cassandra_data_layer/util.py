from collections.abc import AsyncIterable, AsyncIterator, Callable
from typing import Any


async def amap[T, R](
    func: Callable[[T], R], items: AsyncIterable[T]
) -> AsyncIterator[R]:
    async for item in items:
        yield func(item)


async def achain_from[T](
    iterables: AsyncIterable[AsyncIterable[T]],
) -> AsyncIterator[T]:
    async for iterable in iterables:
        async for item in iterable:
            yield item


async def achain[T](*iterables: AsyncIterable[T]) -> AsyncIterator[T]:
    for iterable in iterables:
        async for item in iterable:
            yield item


async def apushback[T](item: T, async_iterable: AsyncIterable[T]) -> AsyncIterator[T]:
    yield item
    async for elem in async_iterable:
        yield elem


async def dedupe_iterable(
    iterator: AsyncIterable[Any], key_func: Callable[[Any], Any], duplicates
) -> AsyncIterator[Any]:
    seen_keys = set()
    async for item in iterator:
        key = key_func(item)
        if key in seen_keys:
            duplicates.append(item)
            continue
        seen_keys.add(key)
        yield item


async def afilter(
    predicate: Callable[[Any], bool],
    async_iterable: AsyncIterable[Any],
) -> AsyncIterator[Any]:
    async for item in async_iterable:
        if predicate(item):
            yield item


async def aempty() -> AsyncIterator[Any]:
    if False:
        yield
