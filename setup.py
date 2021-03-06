from setuptools import setup, find_packages
import os

requirements = [r for r in open('requirements.txt', 'r').read().split('\n') if r]

setup(name = 'presscontrol',
    version = '0.1',
    description = "PressControl is a news database manager capable of scraping and saving articles from twitter pages.",
    #long_description = '',
    keywords = 'press news scraper news extractor twitter mysql information retrieval',
    author = 'Emilio Bravo',
    author_email = 'ebravofm@gmail.com',
    url = 'https://github.com/ebravofm/PressControl',
    license = 'Apache License 2.0',
    packages = find_packages(),
    include_package_data = True,
    zip_safe = True,
    entry_points = {'console_scripts': ['presscontrol = presscontrol.__main__:main',],},
    data_files = [(os.environ['HOME']+'/.config/presscontrol/', ['presscontrol/config.yaml'])],
    install_requires = requirements
    
)
