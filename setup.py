from setuptools import setup, find_packages

setup(
    name='Epiviz File Parser',
    version= '0.1',
    description='A python library to interact and compute data from various genomic file formats.',
    long_description='The library supports reading data from various genomic file formats as pandas dataframe. The library supports data from both database and files. One can also compute statistical methods(from numpy) over these datasets',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Topic :: Scientific/Engineering :: Visualization',
    ],
    url='https://github.com/epiviz/epivizFileParser',
    download_url='https://github.com/epiviz/epivizFileParser',
    author='Jayaram Kancherla',
    author_email='jayaram.kancherla@gmail.com',
    packages=find_packages(include=['epiviz*']),
    test_suite="tests",
    setup_requires=["pytest-runner"],
    tests_require=["pytest", "pytest-cov"],
    include_package_data=True,
    license='MIT',
    keywords='bioinformatics genomics visualization computation'
)

