from setuptools import find_packages, setup

setup(
    name='PSLibrary',
    packages=find_packages(include=["PSLibrary"]),
    version='0.1.0',
    description='Useful scripts for PTV Professional Services',
    install_requires=["pandas"],
    author='AhTe',
    license='MIT',
)

# python setup.py bdist_wheel
