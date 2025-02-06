import os
import resource

import psutil


def configured_memory() -> int:
    """
    Get the system's configured memory, respecting RLIMIT_RSS if set.
    """
    soft, hard = resource.getrlimit(resource.RLIMIT_RSS)
    vm = psutil.virtual_memory()
    print(soft, hard, vm.total)
    if soft > 0 and soft < vm.total:
        return soft
    else:
        return vm.total


def memory_limit(fraction=0.5, max_gb=96) -> int:
    """
    Compute a memory limit (in bytes) based on a target fraction of memory and a max limit.

    Args:
        fraction:
            The fraction of system memory to use as a memory limit.
        max_gb:
            The maximum limit to return, in GiB.
    """

    max_lim = max_gb * 1024 * 1024 * 1024
    lim = configured_memory() * fraction
    if lim > max_lim:
        return max_lim
    else:
        return round(lim)


def duck_options(mem_fraction=0.5, mem_max_gb=96) -> dict:
    # num_cores = os.cpu_count()
    num_threads = 8

    mem = memory_limit(mem_fraction, mem_max_gb)

    return {
        "memory_limit": "{:.1f} GiB".format(mem / (1024 * 1024 * 1024)),
        "threads": num_threads,
    }


if __name__ == "__main__":
    print("memory limit:", memory_limit())
    print("DuckDB options:", duck_options())
