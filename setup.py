from setuptools import setup, find_packages

setup(
    name='canvas-data-sdk',
    version='0.1.0',
    author='Colin Murtaugh',
    author_email='cmurtaugh@gmail.com',
    packages=find_packages(),
    scripts=['bin/canvas_data_example.py'],
    url='http://pypi.python.org/pypi/canvas-data-sdk/',
    license='LICENSE.txt',
    description='A Python SDK for working with Instructure\'s Canvas Data REST API.',
    long_description=open('README.txt').read(),
    install_requires=[
        "requests >= 2.13.0",
    ],
)
