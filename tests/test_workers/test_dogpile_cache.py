# SPDX-License-Identifier: GPL-3.0-or-later
import pytest

from iib.workers.dogpile_cache import generate_cache_key


@pytest.mark.parametrize(
    "args,kwargs",
    [
        (['a', 'r', 'g', 's'], {"k": "kwargs"}),
        (
            [
                "Lorem ipsum dolor sit amet, consectetuer adipiscing elit. ",
                "Aenean commodo ligula eget dolor. Aenean massa. Cum sociis ",
                "natoque penatibus et magnis dis parturient montes, nascetur ",
                "ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu",
                "pretium quis, sem. Nulla consequat massa quis enim. Donec.",
            ],
            {"k": "kwargs"},
        ),
        (
            ['a', 'r', 'g', 's'],
            {
                "long": """Lorem ipsum dolor sit amet, consectetuer adipiscing elit.
           Aenean commodo ligula eget dolor. Aenean massa. Cum sociis
           natoque penatibus et magnis dis parturient montes, nascetur""",
                "kwargs": """ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu,
           pretium quis, sem. Nulla consequat massa quis enim. Donec.""",
            },
        ),
    ],
)
def test_generate_cache_key(args, kwargs):
    passwd = generate_cache_key('function_name', *args, **kwargs)
    assert len(passwd) <= 250
