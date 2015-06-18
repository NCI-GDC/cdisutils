from setuptools import setup

setup(
    name="cdisutils",
    version="0.1.0",
    description="Miscellaneous utilities useful for interaction with CDIS systems.",
    license="Apache",
    packages=["cdisutils"],
    install_requires=[
        'apache-libcloud==0.15.1',
        'boto==2.36.0',
    ]
)
