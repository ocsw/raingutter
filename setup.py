#!/usr/bin/env python

# bootstrap setuptools
from ez_setup import use_setuptools
use_setuptools()

# the rest of the imports
import os
from setuptools import setup, find_packages

# so we can put the long description in a README file; see
# http://pythonhosted.org/an_example_pypi_project/setuptools.html
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    # required and packaging/installation info
    name='raingutter',
    version='0.9',
    packages=['raingutter'],
    #package_dir={},
    #py_modules=[],
    install_requires=['nori', 'phpserialize'],
    #extras_require={},
    entry_points={
        'console_scripts': [
            'raingutter = raingutter:main',
            'ocs2drupal = raingutter.ocs2drupal:main',
        ],
        #'gui_scripts': [
        #]
    },

    # PyPI metadata
    description='A diff/sync tool for MySQL databases, including Drupal.',
    long_description=read('README'),
    author='Danielle Malament',
    author_email='danielle.malament@gmail.com',
    url='http://www.obsessivecompulsivesoftware.com/',
    #download_url='',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.2',
        'Topic :: Database',
        'Topic :: System :: Archiving :: Mirroring',
        'Topic :: Utilities',
    ],
    #license='',  # only if it's not in the classifiers
    keywords='database, MySQL, Drupal, diff, sync, mirror',
)
