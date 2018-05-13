from shutil import rmtree
from random import randint
from functools import reduce
from pymongo import MongoClient
from glob import glob
from os.path import join
from psutil import Process
from subprocess import Popen
import time
import redis
import shlex
import codecs
import json
import os

##
# Config helpers
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
    'mongo_url': 'mongodb://localhost:27017/intertext',
    'redis_url': 'redis://localhost:6379/0',
    'max_ram': '4gb',
    'tmp': 'tmp',
    'clear_tmp_files': True,
    'clear_db': True,
    'save_to': 'mongo',
    'start_celery': True,
  }
  config = load_json('config.json')
  for k in defaults:
    if not k in config:
      config[k] = defaults[k]
  return config


def get_db():
  '''
  Return a MongoDB connection for saving results.
  @returns:
    the database indicated in the mongo_url
  '''
  client = MongoClient(config['mongo_url'])
  return client.get_default_database()


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


##
# I/O helpers
##

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
# Celery helpers
##

def start_celery():
  '''
  Start the celery deamon.
  @returns:
    subprocess.Popen object that supports pid lookup via
    self.pid
  '''
  cmd = 'celery worker '
  cmd += '--app intertext.tasks '
  cmd += '--loglevel error'
  return Popen(shlex.split(cmd))


def terminate_process(pid):
  '''
  Stop a process given its pid or stop all processes if pid
  is a list of identifiers.
  @args:
    int pid: the process identifier of the root process that
      spawned child celery processes
  '''
  process = Process(pid)
  for child in process.children(recursive=True):
    child.kill()
  process.kill()

##
# Redis helpers
##

def get_redis():
  '''
  Create and return a Redis connection.
  @returns:
    redis.StrictRedis instance, which supports redis API commands.
  '''
  r = redis.StrictRedis().from_url(config['redis_url'])
  r.config_set('maxmemory', config['max_ram'])
  r.config_set('maxmemory-policy', 'noeviction')
  r.flushdb()
  return r


def execute_pipe(table, pipeline):
  '''
  Try to execute the operations in a Redis pipeline.
  If the request fails because Redis has reached the
  max RAM allocated to it, write Redis key values
  to disk then run the pipeline.
  @params:
    str table: the 'table' in which all pipeline records are stored
    redis.Redis.pipeline pipeline: a pipe of commands to execute
  '''
  while True:
    try:
      return pipeline.execute()
    # if another process is writing, sleep, else write
    except redis.exceptions.ResponseError:
      # another process is writing; sleep and try again
      if r.get('writelock').decode('utf-8') == 'true':
        time.sleep(1)
        continue
      # no process is writing; appoint this process the writer
      if table == 'hashband':
        write_redis_hashbands()
      elif table == 'candidates':
        write_redis_candidates()
      # execute the pipe this process was sent to execute
      return execute_pipe(table, pipeline)


def write_redis_hashbands():
  '''
  Perform atomic read + delete operation on all key / value
  pairs in the Redis hashband table.
  @params:
    str table: the prefix added to all hashband keys
  '''
  if redis_is_write_locked(): return
  r.set('writelock', 'true')
  print(' * writing hashbands to disk')

  for k in r.keys('hashband-*'):
    smembers, _ = r.pipeline().smembers(k).delete(k).execute()
    smembers = [i.decode('utf-8') for i in smembers]
    key = k.decode('utf-8').split('hashband-')[1]
    # a and b represent the letters 0+1 and 2+3 in hashband
    # these are used to create a simple tree structure on disk
    # for quick hashband retrieval
    a, b = key[0:2], key[2:4]
    out_dir = os.path.join(config['tmp'], 'hashbands', a, b)
    out_path = os.path.join(out_dir, a + b)
    make_dirs(out_dir)
    write(out_path, '#'.join(smembers) + '#')
  r.set('writelock', 'false')


def write_redis_candidates():
  '''
  Perform atomic read + delete operation on all key / value
  pairs in the Redis candidates table.
  '''
  if redis_is_write_locked(): return
  r.set('writelock', 'true')
  print('* writing match candidates to disk')

  for k in r.keys('candidates-*'):
    smembers, _ = r.pipeline().smembers(k).delete(k).execute()
    smembers = [i.decode('utf-8') for i in smembers]
    file_id = k.decode('utf-8').split('candidates-')[1]
    out_dir = join(config['tmp'], 'candidates')
    out_path = join(out_dir, file_id)
    make_dirs(out_dir)
    write(out_path, '#'.join(smembers) + '#')


def redis_is_write_locked():
  '''
  Determine whether a process is currently writing to Redis.
  '''
  result = r.get('writelock')
  if result and result.decode('utf-8') == 'true':
    return True
  return False


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

##
# Other helpers
##

def limit_float_precision(f):
  '''
  Limit the number of decimal values in a floating point value.
  @args:
    float f: a float point number
  @returns:
    float: the same input number with fewer decimal digits
  '''
  return int((f * 100) + 0.5) / 100.0

##
# Globals
##

config = get_config()
infiles = glob(config['infiles'])
text_ids = [str(i) for i in range(len(infiles))]
r = get_redis()