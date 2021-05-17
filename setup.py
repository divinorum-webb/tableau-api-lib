"""
Run the following commands to manually deploy:
python setup.py sdist bdist_wheel
python -m twine upload dist/*
pip install --upgrade tableau-api-lib
"""

import setuptools


with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tableau_api_lib",
    version="0.1.10",
    author="Elliott Stam",
    author_email="elliott.stam@gmail.com",
    description="This library enables developers to call any method seen in Tableau Server's REST API documentation.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/divinorum-webb/tableau-api-lib",
    packages=setuptools.find_packages('src'),
    package_dir={
        '': 'src',
    },
    install_requires=[
        'requests>2',
        'urllib3',
        'pandas',
        'requests-toolbelt>=0.9.0',
        'bleach>=3.0',
        'typeguard'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)

