from os import path

from setuptools import find_packages, setup  # Always prefer setuptools over distutils

here = path.abspath(path.dirname(__file__))


setup(
    name="""ckanext-ogdchcommands""",
    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # http://packaging.python.org/en/latest/tutorial.html#version
    version="0.0.1",
    description="""Commands for opendata.swiss""",
    # The project's main homepage.
    url="https://github.com/ogdch/ckanext-ogdchcommands",
    # Author details
    author="""Liip AG""",
    author_email="""ogdch@liip.ch""",
    # Choose your license
    license="AGPL",
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        # 3 - Alpha
        # 4 - Beta
        # 5 - Production/Stable
        "Development Status :: 4 - Beta",
        # Pick your license as you wish (should match "license" above)
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        "Programming Language :: Python :: 2.7",
    ],
    # What does your project relate to?
    keywords="""CKAN commands harvest shacl validation""",
    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=["contrib", "docs", "tests*"]),
    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points="""
    [ckan.plugins]
        ogdch_cmd=ckanext.ogdchcommands.plugin:OgdchCommandsPlugin
        ogdch_admin=ckanext.ogdchcommands.plugin:OgdchAdminPlugin
    """,
)
