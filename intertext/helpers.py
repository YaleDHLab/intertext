from shutil import rmtree
from random import randint
from functools import reduce
from glob import glob
from os.path import join
from psutil import Process
from subprocess import Popen
from redlock import Redlock
from pymongo import MongoClient
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
  r.delete('writelock')
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
    # if Redis is using too much RAM write to disk
    if r.info()['used_memory_rss'] >= max_redis_bytes * .1:
      save_redis_table(table)
    # pipes empty on error, so execute on a copy
    try:
      pipe = pipeline
      pipe.execute()
      break
    # catch OOM from Redis
    except Exception as exc:
      print(' ! error occurred when executing pipe', err)


def save_redis_table(table):
  '''
  Save all records in a redis table (i.e. all records whose
  keys contain `table` as a prefix) to disk, then delete those
  keys to free up RAM.
  @params:
    str table: the 'table' in which all pipeline records are stored
  '''
  try:
    # dlm returns a Lock or False if a Lock already exists
    lock = dlm.lock('writelock', 1000 * 100)
    if lock:
      if table == 'hashband':
        write_redis_hashbands()
      elif table == 'candidates':
        write_redis_candidates()
      dlm.unlock(lock)
  except Exception as exc:
    print(' ! error occurred when writing redis to disk', err)


def write_redis_hashbands():
  '''
  Perform atomic read + delete operation on all key / value
  pairs in the Redis hashband table.
  @params:
    str table: the prefix added to all hashband keys
  '''
  print(' * writing hashbands to disk')
  pipe = r.pipeline()
  keys = []
  for i in r.keys('hashband-*'):
    key = i.decode('utf8')
    keys.append(key.split('hashband-')[1])
    pipe.smembers(key).delete(key)
  results = pipe.execute()

  # results stores [smembers, del, smembers, del] results
  smember_results = [i for idx, i in enumerate(results) if idx%2 == 0]
  for key, smembers in zip(keys, smember_results):
    smembers = [i.decode('utf8') for i in smembers]
    a, b = key[0:2], key[2:4]
    out_dir = os.path.join(config['tmp'], 'hashbands', a, b)
    out_path = os.path.join(out_dir, a + b)
    make_dirs(out_dir)
    write(out_path, '#'.join(smembers) + '#')


def write_redis_candidates():
  '''
  Perform atomic read + delete operation on all key / value
  pairs in the Redis candidates table.
  '''
  print(' * writing match candidates to disk')
  for k in r.keys('candidates-*'):
    smembers, _ = r.pipeline().smembers(k).delete(k).execute()
    smembers = [i.decode('utf-8') for i in smembers]
    file_id = k.decode('utf-8').split('candidates-')[1]
    out_dir = join(config['tmp'], 'candidates')
    out_path = join(out_dir, file_id)
    make_dirs(out_dir)
    write(out_path, '#'.join(smembers) + '#')


def parse_redis_url():
  '''
  Parse config.redis_url into host, port, and db args,
  and return a dictionary containing each value
  '''
  return {
    'host': config['redis_url'].split('redis://')[1].split(':')[0],
    'port': config['redis_url'].split(':')[2].split('/')[0],
    'db':   config['redis_url'].split('/')[-1],
  }


def get_max_redis_ram():
  '''
  Return the max number of bytes Redis can use
  '''
  max_human = config['max_ram']
  if 'gb' in max_human:
    base = 'gb'
    multiplier = 1000 * 1000 * 1000
  elif 'mb' in max_human:
    base = 'mb'
    multiplier = 1000 * 1000
  elif 'kb' in max_human:
    base = 'kb'
    multiplier = 1000
  return float(max_human.replace(base, '')) * multiplier


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
dlm = Redlock([parse_redis_url()])
max_redis_bytes = get_max_redis_ram()