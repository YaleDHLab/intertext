from setuptools import setup
import os, sys

# check Python version
if sys.version_info >= (3,8):
  sys.exit('Sorry, Intertext requires Python 3.7 or earlier')

setup(
  name='intertext',
  version='0.0.1',
  packages=['intertext'],
  keywords = ['text-mining', 'data-visualization', 'text-reuse', 'plagiarism'],
  description='Discover and visualize text reuse',
  url='https://github.com/yaledhlab/intertext',
  author='Yale DHLab',
  author_email='douglas.duhaime@gmail.com',
  license='MIT',
  install_requires=[
    'beautifulsoup4==4.5.1',
    'datasketch==0.2.6',
    'networkx>=2.4',
    'nltk==3.4.5',
    'numpy>=1.18.0',
    'requests==2.24.0',
    'unidecode==1.2.0',
    'vectorizedMinHash>=0.0.2'
  ],
  dependency_links=[
    'git+https://github.com/yaledhlab/vectorized-minhash.git@0.0.2#egg=vectorizedMinHash-0.0.2'
  ],
  entry_points={
    'console_scripts': [
      'intertext=intertext:parse',
    ],
  },
)
