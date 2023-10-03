import os
from setuptools import setup, find_packages


def read(filename):
    return open(os.path.join(os.path.dirname(__file__), filename)).read()


def get_version(version_file):
    ns = {}
    exec(read(version_file), ns)
    return ns['__version__']


setup(
    name='as_client',
    version=get_version('as_client/version.py'),
    author='Mac Coombe',
    author_email='mac.coombe@csiro.au',
    description='Analysis Services Client',
    test_suite="test",
    # TODO: license = '',
    keywords='Analysis-Services',
    url='https://bitbucket.csiro.au/projects/SC/repos/as-client-python/browse',
    packages=find_packages(),
    long_description=read('readme.md'),
    install_requires=[
        'requests >= 2.11.1'
    ],
    tests_require=[
        'responses >=0.13.2,<1.0.0'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Scientific/Engineering :: Information Analysis',
        # TODO: 'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 2.7'
    ]
)
