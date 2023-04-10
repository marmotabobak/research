from threading import Thread
import time
import asyncio
from typing import Callable
from multiprocessing import Process

import tqdm


def count_time(f: Callable) -> Callable:
    def wrapper(*args, **kwargs):
        s_t = time.time()
        res = f(*args, **kwargs)
        print(f'[x] {time.time() - s_t} seconds')
        return res
    return wrapper


def task_cpu_or_sleep(pb, cpu_operations: int = None, time_to_sleep: float = None) -> None:
    if cpu_operations:
        for _ in range(cpu_operations):
            pass
    if time_to_sleep:
        time.sleep(time_to_sleep)
    pb.update(1)


@count_time
def regular_process(pb, cycles: int, cpu_operations_per_cycle: int = None, tts: float = None) -> None:
    kwargs = {'pb': pb}

    if cpu_operations_per_cycle:
        kwargs['cpu_operations'] = cpu_operations_per_cycle

    if tts:
        kwargs['time_to_sleep'] = tts

    for _ in range(cycles):
        task_cpu_or_sleep(**kwargs)


@count_time
def thread_process(pb, cycles: int, cpu_operations_per_cycle: int = None, tts: float = None):

    if cpu_operations_per_cycle:
        args = (pb, cpu_operations_per_cycle, None)

    if tts:
        args = (pb, None, tts)

    threads = [Thread(target=task_cpu_or_sleep, args=args) for _ in range(cycles)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


async def coro_cpu_or_sleep(pb, cpu_operations: int = None, time_to_sleep: int = None):
    pb.update(1)
    if cpu_operations:
        for _ in range(cpu_operations):
            pass
    else:
        await asyncio.sleep(time_to_sleep)


async def asyncio_main(pb, cn, cpu_operations_per_cycle: int = None, tts: float = None):

    kwargs = {'pb': pb}

    if cpu_operations_per_cycle:
        kwargs['cpu_operations'] = cpu_operations_per_cycle

    if tts:
        kwargs['time_to_sleep'] = tts

    for _ in range(cn):
        asyncio.create_task(coro_cpu_or_sleep(**kwargs))


@count_time
def asyncio_process(cycles: int, pb, cpu_operations_per_cycle: int = None, tts: float = None):
    asyncio.run(asyncio_main(cn=cycles, pb=pb, cpu_operations_per_cycle=cpu_operations_per_cycle, tts=tts))


def multiprocessing_task(cpu_operations: int) -> None:
    for _ in range(cpu_operations):
        pass


@count_time
def multiprocessing_process(cycles: int, cpu_operations: int):

    processes = [Process(target=multiprocessing_task, args=(cpu_operations, )) for _ in range(cycles)]
    for process in processes:
        process.start()
    for process in processes:
        process.join()


def run_all(title: str, cycles: int, cpu_operations_per_cycle: int = None, time_to_sleep: float = None, multiprocessing: bool = False):
    print(title)
    functions = {
        'Regular': regular_process,
        'Threading': thread_process,
        'Asyncio': asyncio_process,
    }
    if multiprocessing:
        print('Multiprocessing: ...')
        multiprocessing_process(cycles=cycles, cpu_operations=cpu_operations_per_cycle)
    for name, func in functions.items():
        progress_bar = tqdm.tqdm(desc=name, total=cycles)
        func(pb=progress_bar, cycles=cycles, cpu_operations_per_cycle=cpu_operations_per_cycle, tts=time_to_sleep)
        progress_bar.close()


if __name__ == '__main__':

    max_total_operations_per_cycle = 10_000_000

    if max_total_operations_per_cycle:
        for operations_num in range(1_000_000, max_total_operations_per_cycle + 1_000_000, 1_000_000):
            cycles_num = int((max_total_operations_per_cycle - operations_num) / 1_000_000)
            run_all(
                title=f'\n--- CPU-bound ({cycles_num} cycles/cores) with {operations_num} operations---',
                cycles=cycles_num,
                cpu_operations_per_cycle=operations_num,
                multiprocessing=True
            )

    cycles = 100
    min_time_to_sleep = 0.01
    max_time_to_sleep = 0.1

    if cycles and min_time_to_sleep:
        time_to_sleep = min_time_to_sleep
        while time_to_sleep < max_time_to_sleep:
            run_all(
                title=f'\n--- I/O-bound ({cycles} sleeps for ({time_to_sleep} sec.)) ---',
                cycles=cycles,
                time_to_sleep=time_to_sleep
            )
            time_to_sleep += (max_time_to_sleep - min_time_to_sleep) / 10
