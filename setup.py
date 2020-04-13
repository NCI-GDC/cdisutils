from setuptools import setup

setup(
    name="cdisutils",
    version="0.1.1",
    description="Miscellaneous utilities useful for interaction with CDIS systems.",
    license="Apache",
    packages=["cdisutils"],
    install_requires=[
        'setuptools',
        'attrs==19.1.0',
        'xmltodict==0.9.2',
        'pyOpenSSL==17.5.0',
        'openpyxl==2.4.0',
        'ndg-httpsclient==0.4.3',
    ],
)
