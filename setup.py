import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tableau_api_lib",
    version="0.0.1",
    author="Elliott Stam",
    author_email="elliott.stam@gmail.com",
    description="This library enables developers to call any method seen in Tableau Server's REST API documentation.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/divinorum-webb/tableau-api-lib",
    packages=[
        'tableau_api_lib',
        'decorators',
        'api_endpoints',
        'exceptions',
        'api_requests',
        'sample'
    ],
    package_dir={
        'tableau_api_lib': 'src/tableau_api_lib',
        'decorators': 'src/tableau_api_lib/decorators',
        'api_endpoints': 'src/tableau_api_lib/api_endpoints',
        'exceptions': 'src/tableau_api_lib/exceptions',
        'api_requests': 'src/tableau_api_lib/api_requests',
        'sample': 'src/tableau_api_lib/sample'
    },
    install_requires=[
        'requests>2',
        'urllib3',
        'requests-toolbelt>=0.9.0',
        'bleach>=3.0'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
