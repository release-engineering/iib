# SPDX-License-Identifier: GPL-3.0-or-later
import functools
import hashlib
from typing import Callable

from dogpile.cache import make_region
from dogpile.cache.region import CacheRegion

from iib.workers.config import get_worker_config


def skopeo_inspect_should_use_cache(*args, **kwargs) -> bool:
    """Return true in case this requests can be taken from or stored in cache."""
    return any(arg.find('@sha256:') != -1 for arg in args)


def dogpile_cache(dogpile_region: CacheRegion, should_use_cache_fn: Callable) -> Callable:
    """
    Dogpile cache decorator.

    :params dogpile_region: Dogpile CacheRegion object
    :params should_use_cache_fn: function which determines if cache should be used
    """

    def cache_decorator(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            should_cache = should_use_cache_fn(*args, **kwargs)
            cache_key = generate_cache_key(func.__name__, *args, **kwargs)

            if should_cache:
                # get data from cache
                output_cache = dogpile_region.get(cache_key)
                if output_cache:
                    return output_cache

            output = func(*args, **kwargs)

            if should_cache:
                dogpile_region.set(cache_key, output)

            return output

        return inner

    return cache_decorator


def generate_cache_key(fn: str, *args, **kwargs) -> str:
    """Generate key that is used in dogpile cache."""
    arguments = '|'.join(
        [str(arg) for arg in args] + [f'{kwarg}={kwargs[kwarg]}' for kwarg in kwargs]
    )
    key_str = f'{fn}|{arguments}'
    try:
        # error: Argument 1 to "sha256" has incompatible type "str"; expected Union[bytes, ...]]
        key = hashlib.sha256(key_str).hexdigest()  # type: ignore
    except TypeError:
        key = hashlib.sha256(key_str.encode('utf-8')).hexdigest()
    return key


def create_dogpile_region() -> CacheRegion:
    """Create and configure a dogpile region."""
    conf = get_worker_config()

    return make_region().configure(
        conf.iib_dogpile_backend,
        expiration_time=conf.iib_dogpile_expiration_time,
        arguments=conf.iib_dogpile_arguments,
    )
