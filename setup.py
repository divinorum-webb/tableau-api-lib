import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tableau-api-lib",
    version="0.0.1",
    author="Elliott Stam",
    author_email="elliott.stam@gmail.com",
    description="This library enables developers to call any method seen in Tableau Server's REST API documentation.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/divinorum-webb/tableau-api-lib",
    packages=setuptools.find_packages(),
    install_requires=[
        'requests',
        'urllib3'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
