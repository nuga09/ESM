from setuptools import setup, find_packages

setup(
    name='nestor',
    version='0.1.0',
    author='Peter Lopion, Felix Kullmann, Rachel Maier',
    author_email="ra.maier@fz-juelich.de",
    description="Module for single node pathway optimization with FINE",
    # url='https://jugit.fz-juelich.de/iek-3/groups/stationary-energy-systems/codenestor/nestor',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        # "numpy",
        # "pandas",
        #  "FINE",
        # "matplotlib",
    ]
)
