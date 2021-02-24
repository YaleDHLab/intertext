from os.path import join, exists, dirname, realpath
from setuptools import setup
import os, sys

# check python version
if sys.version_info >= (3,8):
  sys.exit('Sorry, Intertext requires Python 3.7 or earlier')

# populate list of all paths in `./intertext/web`
web = []
dirs = [join('intertext', 'web')]
for i in dirs:
  for root, subdirs, files in os.walk(i):
    if not files: continue
    for file in files:
      web.append(join(root.replace('intertext/', '').replace('intertext\\',''), file))

setup(
  name='intertext',
  version='0.0.1',
  packages=['intertext'],
  package_data={
    'intertext': web,
  },
  keywords = ['text-mining', 'data-visualization', 'text-reuse', 'plagiarism'],
  description='Discover and visualize text reuse',
  url='https://github.com/yaledhlab/intertext',
  author='Yale DHLab',
  author_email='douglas.duhaime@gmail.com',
  license='MIT',
  install_requires=[
    'beautifulsoup4==4.5.1',
    'datasketch==0.2.6',
    'nltk==3.4.5',
    'pymongo==3.3.1',
  ],
  entry_points={
    'console_scripts': [
      'intertext=intertext:parse',
    ],
  },
)
