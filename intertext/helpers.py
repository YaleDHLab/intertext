from shutil import rmtree
from random import randint
from functools import reduce
from pymongo import MongoClient
from glob import glob
from os.path import join
import codecs
import json
import os

##
# Helpers
##

def get_config():
  '''
  Return an object that outlines the config values for this process
  '''
  defaults = {
    'encoding': 'utf8',
    'xml_tag': False,
    'step': 4,
    'window_size': 14,
    'n_permutations': 256,
    'hashband_length': 4,
    'min_similarity': 0.65,
    'same_author_matches': True,
    'mongo_url': 'mongodb://localhost:27017',
    'redis_url': 'redis://localhost:6379/0',
    'db': 'intertext',
    'tmp': 'tmp',
    'clear_tmp_files': True,
    'clear_db': True,
    'save_to': 'mongo',
  }
  config = load_json('config.json')
  for k in defaults:
    if not k in config:
      config[k] = defaults[k]

  return config

def read(path, encoding='utf8'):
  '''
  Read and return the content at path using a specified encoding
  @args:
    str path: the path to a file
  @returns:
    str: the content of the given file
  '''
  with codecs.open(path, 'r', encoding) as f:
    return f.read()


def load_json(path):
  '''
  Given a path to a JSON file, return that file's content in JSON form
  (as a dict/list).
  @args:
    str path: the path to a JSON file
  @returns:
    dict/list: the file content in JSON form
  '''
  with open(path) as f:
    return json.load(f)


def write(path, content):
  '''
  Write `content` to a file at `path`. NB: write in append mode.
  @args:
    str path: the path to the file to which we'll append content
    str content: the string content to append to that file
  '''
  with codecs.open(path, 'a', 'utf8') as out:
    out.write(content)


def rm_dirs(path):
  '''
  Delete the directory and all subcontent located at `path`
  @args:
    str path: the path to a directory to delete
  '''
  try:
    rmtree(path)
  except:
    pass


def make_dirs(path):
  '''
  Make a new directory at `path` if it doesn't exist
  '''
  try:
    os.makedirs(path)
  except:
    pass


def get_db():
  '''
  Return a MongoDB connection for saving results.
  @returns:
    pymongo.mongo_client.MongoClient
  '''
  return MongoClient(config['mongo_url'])[config['db']]


def save(table_name, obj):
  '''
  Save JSON packets formatted for app consumption in mongo or on disk,
  according to the value of `config.save_to`.
  @args:
    str table_name: the name of the table to which results should be saved
    list/dict obj: a JSON-serializable object (list or dict)
  '''
  db = get_db()

  # save results to mongo
  if config['save_to'] == 'mongo':
    if isinstance(obj, list):
      db[table_name].insert_many(obj)
    else:
      db[table_name].insert_one(obj)

  # save results to disk
  elif config['save_to'] == 'disk':
    out_dir = join(config['tmp'], 'results', table_name)
    out_file = str(randint(0, 2**128)) + '.json'
    make_dirs(out_dir)
    with open(join(out_dir, out_file), 'w') as out:
      json.dump(obj, out)


def get_metadata():
  '''
  Load the metadata file (if provided) or return an empty dictionary
  @returns:
    dict: a dictionary with the infile metadata
  '''
  print(' * loading metadata')
  try:
    return json.load(open(config.get('metadata', '')))
  except:
    print(' ! no metadata file found')
    return {}


def get_nested_path(root_dir, filename):
  '''
  Given a root directory name and the name of the file to be
  stored within that directory, create an outfile path that
  uses the first three characters in filename as nested
  directories in out_dir to minimize files per directory
  @args:
    str root_dir: the name of a directory within config['tmp']
    str filename: the name of a file
  @returns:
    str: the path to `filename` in `dir_name` with several
      intermediary subdirectories
  '''
  out_dirs = [config['tmp'], root_dir]
  out_dirs += [i for i in os.path.basename(filename)[:3]]
  out_dir = reduce(join, out_dirs)
  make_dirs(out_dir)
  return join(out_dir, filename)


##
# Globals
##

config = get_config()
infiles = glob(config['infiles'])
text_ids = [str(i) for i in range(len(infiles))]