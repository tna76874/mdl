
#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
from setuptools import find_packages, setup

import mdl

setup(
    name='mdl',
    version=mdl.__version__,
    description='A Mediathek Downloader',
    url='https://github.com/tna76874/mdl.git',
    author='maaaario',
    author_email='',
    license='BSD 2-clause',
    packages=find_packages(),
    install_requires=[
        "pandas",
        "argparse",
        "requests",
        "python-slugify",
        "beautifulsoup4",
        "SQLAlchemy",
        "packaging",
    ],
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',  
        'Operating System :: POSIX :: Linux',        
        'Programming Language :: Python :: 3.7',
    ],
    python_requires = ">=3.6",
    entry_points={
        "console_scripts": [
            "mdl = mdl.mdl:main",
        ],
    },
    )