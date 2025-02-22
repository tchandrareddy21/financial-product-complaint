from setuptools import setup, find_packages
from typing import List

# Declaring variables for setup
PROJECT_NAME = "Financial Product Complaint"
VERSION = "v0.0.0"
AUTHOR = "Chandra Reddy"
DESRCIPTION = "Complaints can give us insights into problems people are experiencing in the marketplace and help us to undestand the reason and do necessary modification in exisiting financial product if required."
REQUIREMENTS_FILE = "requirements.txt"

HYPHEN_E_DOT = "-e ."

def get_requirements_list() -> List[str]:
    """
    Description: This function is going to return list of requirement
    mention in requirements.txt file
    return This function is going to return a list which contain name
    of libraries mentioned in requirements.txt file
    """
    with open(REQUIREMENTS_FILE, "r") as file_obj:
        requirements_list = file_obj.read().splitlines()
        if HYPHEN_E_DOT in requirements_list:
            requirements_list.remove(HYPHEN_E_DOT)
        return requirements_list



setup(
    name = 'Financial Product Complaint',
    version= '0.0.1',
    author= 'Tiyyagura Chandra Reddy',
    author_email= "tchandrareddy21@gmail.com",
    packages = find_packages(),
    install_requires = get_requirements_list()
)


