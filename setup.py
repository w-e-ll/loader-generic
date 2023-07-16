import os

from glob import glob
from setuptools import setup, find_packages

from loader_generic import VERSION

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='loader-generic',
    description="Loader-generic development",
    version=VERSION,
    long_description=README,
    author='Valentin Sheboldaev',
    classifiers=[
        'Development Status :: 5 - Production',
        'Environment :: Console',
        'License :: Other/Proprietary License',
        'Natural Language :: English',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX :: Linux',
        'Operating System :: Unix',
        'Programming Language :: Python :: 3.10.4'
    ],
    packages=find_packages(),
    data_files=[
        ('', glob('*.py')),
        ('', glob('*.txt')),
    ],
    include_package_data=True,
    platforms=['Any'],
    zip_safe=False,
    install_requires=[
    ],
    entry_points={
        'console_scripts': [
            'loader.py=loader_generic.scripts.loader:main'
        ],

    }
)


