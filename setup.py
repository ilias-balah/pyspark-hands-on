from os.path import dirname, join, isfile
from setuptools import setup, find_packages

def read_requirements():
    """
    Reads the requirements.txt file and returns a list of required packages.
    """
    # Read the requirements from the requirements.txt file
    requirements_path = join(dirname(__file__), 'requirements.txt')

    # If the requirements file does not exist, return an empty list
    requirements = []

    # Check if the file exists, and if it does, read the requirements
    if isfile(requirements_path):
        with open(requirements_path, 'r+') as f:
            requirements = [ req.strip() for req in f if req.strip() and not req.startswith('#') ]

    return requirements

setup(
    name='spark-handson',
    version='0.1',
    author='Ilias',
    packages=['src.internal'],
    install_requires=read_requirements()
)