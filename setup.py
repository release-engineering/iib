# SPDX-License-Identifier: GPL-3.0-or-later
from setuptools import setup, find_packages

setup(
    name='iib',
    version='2.1.0',
    long_description=__doc__,
    packages=find_packages(exclude=['tests', 'tests.*']),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'celery',
        'flask',
        'flask-login',
        'flask-migrate',
        'flask-sqlalchemy',
        'psycopg2-binary',
        'requests',
        'requests-kerberos',
    ],
    classifiers=[
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    entry_points={'console_scripts': ['iib=iib.web.manage:cli']},
    license="GPLv3+",
    python_requires='>=3.5',
)
