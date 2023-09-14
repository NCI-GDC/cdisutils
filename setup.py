from setuptools import setup

with open("README.md") as readme_in:
    long_description = readme_in.read()


setup(
    name="cdisutils",
    setup_requires=["setuptools_scm<6"],
    use_scm_version={"local_scheme": "dirty-tag"},
    author="Center for Translational Data Science",
    author_email="support@nci-gdc.datacommons.io",
    description="Miscellaneous utilities useful for interaction with CTDS systems.",
    url="https://github.com/NCI-GDC/cdisutils",
    license="Apache-2.0",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: Apache Software License",
    ],
    python_requires=">=3.7, <4",
    packages=["cdisutils"],
    # Note that some of these ranges have generous lower bounds since some
    # consumers of cdisutils might still expect older versions.
    install_requires=[
        "boto3~=1.26",
        "python-dateutil~=2.4",
        "urllib3>=1.26,<3",
    ],
    # Some lesser-used parts of cdisutils require extra dependencies.
    extras_require={
        # cdisutils.dbgap
        # If you include this extra, your application/library should specify
        # the expected versions of gdcdatamodel2 and the associated dictionary.
        "dbgap": ["gdcdatamodel2", "requests>=2.7", "xmltodict>=0.9"],
        #
        # cdisutils.excel
        "excel": ["openpyxl~=2.4"],
        #
        # bin/nova_status.py
        "nova": ["python-novaclient~=3.2"],
        "dev": [
            "moto~=4.1",
            "requests~=2.31",
            "pytest>4.6",
            "pytest-cov>2.10",
            "flask~=2.0",  # required for moto_server
            "flask_cors~=3.0",  # required for moto_server
        ],
    },
)
