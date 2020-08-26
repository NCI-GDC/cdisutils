from setuptools import setup


with open("README.md") as readme_in:
    long_description = readme_in.read()


setup(
    name="cdisutils",
    setup_requires=["setuptools_scm"],
    use_scm_version={"local_scheme": "dirty-tag"},
    author="Center for Translational Data Science",
    author_email="support@nci-gdc.datacommons.io",
    description="Miscellaneous utilities useful for interaction with CTDS systems.",
    url="https://github.com/NCI-GDC/cdisutils",
    license="Apache-2.0",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "License :: OSI Approved :: Apache Software License",
    ],
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, <4',
    packages=["cdisutils"],
    # Note that some of these ranges have generous lower bounds since some
    # consumers of cdisutils might still expect older versions.
    install_requires=[
        "boto~=2.36",
        "boto3~=1.9",
        "future>=0.16,<1.0",
        "python-dateutil~=2.4",
        "PyYAML>=3.11<6.0",
        "six~=1.11",
        "urllib3~=1.0",
    ],
    # Some lesser-used parts of cdisutils require extra dependencies.
    extras_require={
        # cdisutils.dbgap
        # If you include this extra, your application/library should specify
        # the expected versions of gdcdatamodel and the associated dictionary.
        "dbgap": ["gdcdatamodel", "requests~=2.7", "xmltodict~=0.9"],
        #
        # cdisutils.excel
        "excel": ["openpyxl~=2.4"],
        #
        # bin/nova_status.py
        "nova": ["python-novaclient~=3.2"],
    },
)
