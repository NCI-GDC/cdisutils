from setuptools import setup

setup(
    name="cdisutils",
    version="0.1.1",
    description="Miscellaneous utilities useful for interaction with CDIS systems.",
    license="Apache",
    packages=["cdisutils"],
    install_requires=[
        'setuptools==30.1.0',
        'boto==2.36.0',
        'xmltodict==0.9.2',
        'python-dateutil==2.4.2',
        'pyOpenSSL==16.2.0',
        'openpyxl==2.4.0',
        'ndg-httpsclient==0.4.3',
        'signpostclient'
    ],
    dependency_links = [
        'git+ssh://git@github.com/NCI-GDC/python-signpostclient.git@ca686f55772e9a7f839b4506090e7d2bb0de5f15#egg=signpostclient',
    ]

)
