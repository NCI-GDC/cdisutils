from setuptools import setup

setup(
    name="cdisutils",
    version="1.2.2",
    description="Miscellaneous utilities useful for interaction with CDIS systems.",
    license="Apache",
    packages=["cdisutils"],
    install_requires=[
        'xmltodict>=0.9.2',
        'pyOpenSSL>=16.2.0',
        'openpyxl>=2.4.0',
        'ndg-httpsclient>=0.4.3',
        'boto>=2.36.0',
        'python-dateutil>=2.4.2',
        'indexclient>=1.5.6',
    ],
)
