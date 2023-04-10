from threading import Thread
import time
import asyncio
from typing import Callable

import tqdm


CPU_CYCLES = 10
CPU_OPERATIONS_PER_CYCLE = 1_000_000

IO_CYCLES = 1_000


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


async def coro_cpu_or_sleep(pb, cpu_operations: int = None):
    pb.update(1)
    if cpu_operations:
        for _ in range(cpu_operations):
            pass
    else:
        await asyncio.sleep(0.1)


async def asyncio_main(cn, pb, cpu_operations_per_cycle: int = None):
    for _ in range(cn):
        asyncio.create_task(coro_cpu_or_sleep(pb=pb, cpu_operations=cpu_operations_per_cycle))


@count_time
def regular_process(pb, cycles: int, cpu_operations_per_cycle: int = None) -> None:
    kwargs = {'pb': pb}
    if cpu_operations_per_cycle:
        kwargs['cpu_operations'] = cpu_operations_per_cycle
    else:
        kwargs['time_to_sleep'] = 1 / cycles
    for _ in range(cycles):
        task_cpu_or_sleep(**kwargs)


@count_time
def thread_process(pb, cycles: int, cpu_operations_per_cycle: int = None):

    if cpu_operations_per_cycle:
        args = (pb, cpu_operations_per_cycle, None)
    else:
        time_to_sleep = 1 / cycles
        args = (pb, None, time_to_sleep)

    threads = [Thread(target=task_cpu_or_sleep, args=args) for _ in range(cycles)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


@count_time
def asyncio_process(cycles: int, pb, cpu_operations_per_cycle: int = None):
    asyncio.run(asyncio_main(cn=cycles, pb=pb, cpu_operations_per_cycle=cpu_operations_per_cycle))


if __name__ == '__main__':

    functions = {
        'Regular': regular_process,
        'Threading': thread_process,
        'Asyncio': asyncio_process
    }

    print(f'\n--- CPU-bound ({CPU_CYCLES} cycles with)---')

    for name, func in functions.items():
        progress_bar = tqdm.tqdm(desc=name, total=CPU_CYCLES)
        func(pb=progress_bar, cycles=CPU_CYCLES, cpu_operations_per_cycle=CPU_OPERATIONS_PER_CYCLE)
        progress_bar.close()


    print(f'\n--- I/O-bound ({CPU_CYCLES} sleeps for ({1 / IO_CYCLES} sec.)) ---')

    for name, func in functions.items():
        progress_bar = tqdm.tqdm(desc=name, total=IO_CYCLES)
        func(pb=progress_bar, cycles=IO_CYCLES)
        progress_bar.close()
