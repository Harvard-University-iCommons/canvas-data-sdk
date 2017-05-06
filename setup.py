from setuptools import setup, find_packages

setup(
    name='canvas-data-sdk',
    version='0.1.1',
    author='Colin Murtaugh',
    author_email='cmurtaugh@gmail.com',
    packages=find_packages(),
    include_package_data=True,
    entry_points='''
        [console_scripts]
        canvas-data=canvas_data.scripts.canvasdata:cli
    ''',
    scripts=['bin/canvas_data_example.py'],
    url='http://pypi.python.org/pypi/canvas-data-sdk/',
    license='LICENSE.txt',
    description='A Python SDK for working with Instructure\'s Canvas Data REST API.',
    long_description=open('README.txt').read(),
    install_requires=[
        "requests >= 2.13.0",
        "Click >= 6.7",
        "PyYAML >= "
    ],
)
