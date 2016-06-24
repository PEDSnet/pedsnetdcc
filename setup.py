import sys
from setuptools import setup, find_packages
from pedsnetdcc import __version__

if sys.version_info < (2, 7):
    raise EnvironmentError('Python 2.7.x or greater is required')

with open('README.md', 'r') as f:
    long_description = f.read()

with open('requirements.txt') as f:
    install_requires = f.readlines()

kwargs = {
    'name': 'pedsnetdcc',
    'version': __version__,
    'author': 'The Children\'s Hospital of Philadelphia',
    'author_email': 'cbmisupport@email.chop.edu',
    'url': 'https://github.com/PEDSnet/pedsnetdcc',
    'description': 'CLI tool for PEDSnet data coordinating center ETL tasks',
    'long_description': long_description,
    'license': 'Other/Proprietary',
    'packages': find_packages(),
    'install_requires': install_requires,
    'download_url': ('https://github.com/PEDSnet/'
                     'pedsnetdcc/tarball/%s' % __version__),
    'keywords': ['healthcare', 'ETL', 'data coordinating center'],
    'classifiers': [
        'Development Status :: 4 - Beta',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Healthcare Industry',
        'License :: Other/Proprietary License',
        'Natural Language :: English'
    ],
    'entry_points': {
        'console_scripts': [
            'pedsnetdcc = pedsnetdcc.main:pedsnetdcc'
        ]
    }
}

setup(**kwargs)
