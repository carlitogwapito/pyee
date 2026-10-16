from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from pyee import EventEmitter


def test_concurrent_duplicate_removal_preserves_remaining_listener() -> None:
    ee = EventEmitter()
    calls: list[str] = []

    def removed() -> None:
        calls.append("removed")

    def survivor() -> None:
        calls.append("survivor")

    ee.on("work", removed)
    ee.on("work", survivor)
    ready = Barrier(2)

    def cleanup() -> None:
        ready.wait(timeout=5)
        ee.remove_listener("work", removed)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(cleanup) for _ in range(2)]
        for future in futures:
            future.result(timeout=5)

    assert ee.listeners("work") == [survivor]
    assert ee.emit("work") is True
    assert calls == ["survivor"]
