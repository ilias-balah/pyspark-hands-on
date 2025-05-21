from os.path import dirname, join, isfile
from setuptools import setup, find_packages

def read_requirements():
    """
    Reads the requirements.txt file and returns a list of required packages.
    """
    # Read the requirements from the requirements.txt file
    req_path = join(dirname(__file__), 'requirements.txt')

    # Check if the file exists
    if not isfile(req_path):
        reqs = []
    
    # Read the requirements from the file
    with open('requirements.txt', 'r+') as f:
        reqs = [ req.strip() for req in f if req.strip() and not req.startswith('#') ]

    print(reqs)
    return reqs

setup(
    name='spark-handson',
    version='0.1',
    author='Ilias',
    packages=['src.internal'],
    install_requires=read_requirements()
)