from setuptools import setup

setup(
    name="cdisutils",
    version="0.1.0",
    description="Miscellaneous utilities useful for interaction with CDIS systems.",
    license="Apache",
    packages=["cdisutils"],
    install_requires=[
        'setuptools==30.1.0',
        'xmltodict==0.9.2',
        'pyOpenSSL==16.2.0',
        'ndg-httpsclient==0.4.2'
    ],
)
