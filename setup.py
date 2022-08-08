import pathlib

from setuptools import find_packages, setup

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / 'README.md').read_text(encoding='utf-8')

github_address = 'https://github.com/hit-mc/pypsmb'

setup(
    name='pypsmb',
    version='0.1.2',
    description='psmb server implemented in Python',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url=github_address,
    author='Keuin, inclyc',
    author_email='keuinx@gmail.com, axoford@icloud.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
        'Natural Language :: Chinese (Simplified)',
        'Natural Language :: English',
        'Operating System :: MacOS',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: Utilities',
    ],
    keywords='Chattings, PSMB, Minecraft, BungeeCross, CrossLink',
    package_dir={'pypsmb': 'pypsmb'},
    packages=find_packages('.'),
    python_requires='>=3.8, <4',
    install_requires=['PyYAML'],
    entry_points={
        'console_scripts': [
            'pypsmb=pypsmb:main',
        ],
    },
    project_urls={
        'Bug Reports': github_address + '/issues',
        'Source': github_address,
    },
)
