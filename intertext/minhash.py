from __future__ import division, print_function
from multiprocessing import Pool, cpu_count
from collections import defaultdict
from datasketch import MinHash
from itertools import combinations
from functools import reduce
from difflib import SequenceMatcher
from pymongo import MongoClient
from random import randint
from shutil import rmtree
from nltk import ngrams
from bs4 import BeautifulSoup
from glob import glob
import json
import sys
import os
import time
import codecs
import argparse

##
# Helpers
##

def run_task(function, glob_pattern):
  '''
  Run a task and block all hosts until each host completes the task.
  If files need to be combined, combine those files, and again block
  until all have completed.
  @args:
    function function: a function to run
    str glob_pattern: a pattern that matches one or more directories,
      whose contents will be combined after the function is run
  '''
  function()
  sync_hosts(function.__name__)
  if glob_pattern:
    combine_files(glob_pattern)
    sync_hosts(function.__name__ + '-combine_files')


def sync_hosts(task_id):
  '''
  Helper that syncs all host machines when running a job in a
  multi-host environment. Once each host completes its portion of
  the work on the current task, the host will store its host id
  in a file within config['tmp']/tasks/{{ task_id }}/{{ host_id }}.
  This host will then remain trapped in this function waiting for
  all other hosts to complete their portion of the work, at which
  point they will all be released to pursue the next task.
  @args:
    str task_id: a string that represents the task all host machines
      are currently processing.
  '''
  if not config['multihost']: return
  outdir = os.path.join(config['tmp'], 'tasks', str(task_id))
  make_dirs(outdir)
  host_id = str(config['host_id'])
  append(os.path.join(outdir, host_id), host_id)
  done = len(glob(os.path.join(outdir, '*'))) == config['host_count']
  while not done:
    time.sleep(10)
    done = len(glob(os.path.join(outdir, '*'))) == config['host_count']
  return


def combine_files(glob_pattern):
  '''
  Helper that combines files with identical basenames in a directory.
  To do so, pass all directories that match a glob pattern to a function
  that will combine the contents in a directory whose names are
  identical up to the first # character (as the values after #
  characters in these filenames store only host id and process id
  information for multi-host and multi-processed environments).
  @args:
    str _dir: the path to a directory
    int subdirs: the number of subdirectories to descend into _dir
      when looking for matching files
  '''
  paths = get_host_args(glob(glob_pattern))
  for _ in multiprocess(combine_files_in_dir, paths):
    pass


def get_host_args(arr):
  '''
  If this job is running in a multi-host context, return all
  nth values from `arr`, where `n` = the current host's index position
  in the list of all hosts. If this process is running in a single-host
  context, return arr.
  @args:
    list arr: an iterable
  @returns:
    list an iterable that == `arr` if this process is running on
    a single host, and that is a subset of `arr` if this process
    is running in a multi-host context
  '''
  if config['multihost']:
    host_arr = []
    for idx, i in enumerate(arr):
      # conditionally add this item to the current host's queue
      # NB: we assume host ids use 1-based indexing
      if idx % config['host_count'] == config['host_id'] - 1:
        host_arr.append(i)
    return host_arr
  return arr


def combine_files_in_dir(_dir):
  '''
  Given a dir, combine all files in that dir that share an identical file root
  where root is defined by the string up to the first #
  '''
  for file in glob( os.path.join(_dir, '*#*') ):
    basename = os.path.basename(file).split('#')[0]
    append(os.path.join(_dir, basename), read(file))
    os.remove(file)


def multiprocess(function, args):
  '''
  Given a function and some arguments, multiprocess the args
  through the function and yield results.
  @args:
    function function: a function
    list args: a list of arguments to be passed to the function
  @returns:
    generator: a generator of results from the function
  '''
  pool = Pool(config['max_cores'])
  for result in pool.imap(function, args):
    yield result
  pool.close()
  pool.join()


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


def append(path, content):
  '''
  Append some content to a file at `path`.
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
  host = config['mongo_host']
  port = config['mongo_port']
  return MongoClient(host, port)[config['db']]


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
    out_dir = os.path.join(config['tmp'], 'results', table_name)
    out_file = str(randint(0, 2**128)) + '.json'
    make_dirs(out_dir)
    with open(os.path.join(out_dir, out_file), 'w') as out:
      json.dump(obj, out)


def get_metadata():
  '''
  Load the metadata file (if provided) or return an empty dictionary
  @returns:
    dict: a dictionary with the infile metadata
  '''
  print(' * loading metadata')
  if config.get('metadata', False):
    return load_json(config['metadata'])
  else:
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
  out_dir = reduce(os.path.join, out_dirs)
  make_dirs(out_dir)
  return os.path.join(out_dir, filename)

##
# 1) Hash input texts
##

def hash_inputs():
  '''
  For each input file, partition that text into windows. Hash each
  window, then pass a sliding window over the hashes to get a
  sequence of "hashbands" for each window. Store the hashbands
  that occur in each infile.
  '''
  print(' * writing hashbands')
  args = [[i, idx] for idx, i in enumerate(get_host_args(text_ids))]
  for idx, _ in enumerate(multiprocess(hash_input_file, args)):
    print(' * wrote', idx+1, 'of', len(args), 'hashbands')


def hash_input_file(args):
  '''
  Write a list of hashbands to disk for an infile.
  @args:
    list args:
      str: the text_id of an infile, where each text_id is
        an integer cast as a string that represents the index
        position of an infile within the list of all infiles
      int: an integer representing the index position of a
        process within all processes allocated for this job
        on this host
  '''
  text_id, process_id = args
  text_path = infiles[int(text_id)]
  text_hashbands = []
  # specify the path to the hashbands to be stored
  out_path = get_nested_path('hashbands', os.path.basename(text_path))
  # skip this file if its hashbands are already present
  if os.path.exists(out_path):
    return
  # join the hashbands in each window of the file with *
  for window in get_windows(text_path):
    text_hashbands += list(get_hashbands(window)) + ['*']
  append(out_path, text_id + '=' + '#'.join(text_hashbands))


def get_windows(path):
  '''
  Return a list of strings, where each string represents a "window"
  of text (or sequence of words) from an input file
  @args:
    str path: the path to an input file
  @returns:
    generator: a generator of substrings from the file
  '''
  words = get_text_content(read_input_file(path)).lower().split()
  for window_id, window in enumerate(ngrams(words, config['window_size'])):
    if window_id % config['step'] == 0:
      yield window


def read_input_file(path):
  '''
  Try to read an input file, and return an empty string if that attempt fails
  @args:
    str path: the path to a text file
  @returns:
    str: the file content in string form
  '''
  try:
    return read(path, encoding=config['encoding'])
  except Exception:
    print(' ! warning', path, 'could not be parsed')
    return ''


def get_text_content(s):
  '''
  Given the content from a text file, return that content stripped
  of tags if the user has configured tag stripping, else return
  the string itself.
  @args:
    str s: the text content from a file in string form
  @returns:
    str: the text content from a file with markup tags removed
  '''
  if config['xml_tag']:
    parser = 'html.parser' if infiles[0].split('.')[-1] == '.html' else 'lxml'
    return BeautifulSoup(s, parser).find(config['xml_tag']).get_text()
  return s


def get_hashbands(window):
  '''
  Given a string of content, return a generator of hashbands
  from that string.
  @args:
    str window: a window of text content from a file
  @returns:
    generator: a generator of strings, where each string has the
      format a.b.c in which individual hashes are concatenated
      with periods
  '''
  minhash = MinHash(num_perm=config['n_permutations'], seed=1)
  for ngram in set(ngrams(' '.join(window), 3)):
    minhash.update( ''.join(ngram).encode('utf8') )
  hashband_vals = []
  for i in minhash.hashvalues:
    hashband_vals.append(i)
    if len(hashband_vals) == config['hashband_length']:
      hashband = '.'.join([str(j) for j in hashband_vals])
      hashband_vals = []
      yield hashband

##
# 2) Sort file hashes
##

def sort_hashbands():
  '''
  For each input file, partition that text into windows. Hash each
  window, then pass a sliding window over the hashes to get a
  sequence of "hashbands" for each window. Windows with identical
  hashband sequences are candidates for a match.
  '''
  print(' * sorting hashbands')
  hashbands = glob(os.path.join(config['tmp'], 'hashbands', '*', '*', '*', '*'))
  args = [[i, idx] for idx, i in enumerate(get_host_args(hashbands))]
  for idx, _ in enumerate(multiprocess(sort_file_hashbands, args)):
    print(' * sorted', idx+1, 'of', len(args), 'hashbands')


def sort_file_hashbands(args):
  '''
  Sort the hashbands for an input file by identifying the
  first four characters in each hashband, then writing
  that hashband to config['tmp']/sorted/01/23/hash,
  where 01 and 23 represent characters 01 and 23 from the
  hashband.
  @args:
    list args:
      str: the path to an input file's hashbands file
      int: an integer representing the index position of a
        process within all processes allocated for this job
        on this host
  '''

  hashband_path, process_id = args
  text_id, content = read(hashband_path).split('=')
  for window_id, window in enumerate(content.split('#*#')[:-1]):
    for hashband in window.split('#'):
      if not hashband:
        continue
      # a and b represent the letters 0+1 and 2+3 in hashband
      # these are used to create a simple tree structure on disk
      # for quick hashband retrieval
      a, b = hashband[0:2], hashband[2:4]
      outdir = os.path.join(config['tmp'], 'sorted', a, b)
      make_dirs(outdir)

      filename = a + b + '#' + str(process_id)
      filename += '#' + str(config['host_id']) if config['host_id'] else ''
      content = hashband + '-' + text_id + '.' + str(window_id) + '#'
      append(os.path.join(outdir, filename), content)


##
# 3) Find candidate matches
##

def find_candidate_matches():
  '''
  Step 2 generates a map from hashband to the text id and window ids
  in which that hashband occurs. Step 3 uses those output files
  to identify candidate pairs of text matches. This step is
  separated from Step 4, which validates match pairs, because if we
  can identify all of the candidate matches between two files
  before the validation step, we can drastically reduce i/o.
  '''
  dirs = glob(os.path.join(config['tmp'], 'sorted', '*',))
  args = [[i, idx] for idx, i in enumerate(get_host_args(dirs))]
  for idx, _ in enumerate(multiprocess(find_candidates_in_directory, args)):
    if (idx + 1) % 100 == 0:
      print(' * processed', idx, 'of', len(dirs), 'minhashes')


def find_candidates_in_directory(args):
  '''
  Find candidate matches for text windows by locating hashbands
  that occur in multiple distinct files. For each such hashband,
  find all combinations of text and window ids in which the
  hashband occurs and store them in the following data structure:
  dict[file_id] = [[file_id.window_id, match_file_id.match_window_id], ...]
  Note that given a match between file id's a and b, the match
  will be stored in the list of results for whichever file id is greater.
  Given that dictionary, save all values of each fild_id key to
  a file in tmp/matches/{{ file_id }}
  @args:
    list args:
      str: the path to a directory that is a child directory of
        /tmp/hashbands
      int: the index position of the current process among all processes
  '''
  _dir, process_id = args
  d = defaultdict(list)
  count = 0
  for file in glob(os.path.join(_dir, '*', '*')):
    for matching_ids in get_matching_ids(file):
      # find all combinations of matches between the ids in this match cluster
      for id_pair in combinations(matching_ids, 2):
        # get the file_id and window_id for each id pair
        file_id_a, file_segment_a = id_pair[0].split('.')
        file_id_b, file_segment_b = id_pair[1].split('.')
        # don't allow same-file matches
        if file_id_a != file_id_b:
          # store the match in the larger of the file_ids
          if file_id_a > file_id_b:
            d[file_id_a].append( id_pair[0] + '-' + id_pair[1] )
          else:
            d[file_id_b].append( id_pair[1] + '-' + id_pair[0] )
          count += 1
        if count >= 10000:
          save_candidate_matches(d, process_id)
          d = defaultdict(list)
          count = 0
  if count:
    save_candidate_matches(d, process_id)


def get_matching_ids(file_path):
  '''
  Find the unique hashbands in a hashband file designated by `file_path`,
  and for each, find the list of file_id and window_ids in which
  that hashband occurs. Each hashband file stores data in the following form:
  hashband-file_id.window_id# . Return a list of lists where sublists
  have the format: [file_id_a.window_id_a, file_id_b.window_id_b].
  @args:
    str file_path: the path to a file full of hashband results
  @returns:
    list: a list of lists where sublists detail text windows that share
      a given hashband: [file_id_a.window_id_a, file_id_b.window_id_b]
  '''
  matching_ids = []
  d = defaultdict(list)
  for i in read(file_path).split('#')[:-1]:
    hashband, file_id_window_id = i.split('-')
    d[hashband].append(file_id_window_id)
  # don't return results for hashbands that occur in only one file
  for key in d:
    if len(d[key]) > 1:
      matching_ids.append(d[key])
  return matching_ids


def save_candidate_matches(d, process_id):
  '''
  Save all received candidate matches to disk.
  @args:
    dict d: a dictionary whose keys represent file ids and whose values
      are lists of strings in the following format: [file_id_a.window_id_a,
        file_id_b.window_id_b]
    int process_id: the index position of the current process in the list
      of processes in this job on this host
  '''
  for file_id in d:
    outdir = os.path.join(config['tmp'], 'candidates')
    make_dirs(outdir)
    filename = file_id + '#' + str(process_id)
    if config['host_id']:
      filename += '#' + str(config['host_id'])
    append(os.path.join(outdir, filename), '#'.join(d[file_id]) + '#')


##
# 4) Validate candidate matches
##

def validate_matches():
  '''
  Given the candidate matches identified by Step 2, read in the
  matches for each text pair and validate them with raw string
  similarity lookups
  '''
  match_files = glob(os.path.join(config['tmp'], 'candidates', '*'))
  args = [[i, c] for c, i in enumerate(get_host_args(match_files))]
  for idx, _ in enumerate(multiprocess(validate_text_matches, args)):
    print(' * validated', idx+1, 'of', len(match_files), 'file matches')


def validate_text_matches(args):
  '''
  Validate the candidate matches for a given input file. To do so,
  parse the candidate matches stored in `match_file`, which stores
  candidate matches in the following format:
    file_id.window_id-match_file_id.match_window_id
  To minimize i/o when measuring string similarity, organize these matches
  into a dictionary with format:
    dict[match_file_id] = [(file_segment_id, match_segment_id), ...]
  For each such candidate match, if the similarity is greater than
  `config.min_similarity`, store the match as a validated match within
  tmp/validated/ {{ file_id }} , using the following format:
    file_id.window_id-match_file_id.match_window_id|similarity#
  @args:
    list args:
      str: the path to a candidate match file
      int: the index position of the current process among all processes
  '''
  match_file, process_id = args
  validated = ''
  found = 0
  text_matches = get_text_matches(match_file)
  text_id = os.path.basename(match_file)
  text_windows = list( get_windows(infiles[int(text_id)]) )
  for match_text_id in text_matches:
    match_windows = list( get_windows(infiles[int(match_text_id)]) )
    for text_window_id, match_window_id in text_matches[match_text_id]:
      text_window = ' '.join(text_windows[ text_window_id ])
      match_window = ' '.join(match_windows[ match_window_id ])
      similarity = get_similarity(text_window, match_window)
      # add this validated match to the list of validated matches
      if similarity >= config['min_similarity']:
        file_ids = text_id + '.' + str(text_window_id)
        match_ids = match_text_id + '.' + str(match_window_id)
        validated += file_ids + '-' + match_ids + '|' + str(similarity) + '#'
        found += 1
        if found % 100 == 0:
          print(' * matches found for', match_file, found)
  save_validated_matches(text_id, validated, process_id)


def get_similarity(a, b):
  '''
  Return the similarity of two strings
  @args:
    str a: a string
    str b: a string
  @returns:
    float: a value {0:1} indicating the similarity between `a` and `b`
  '''
  return SequenceMatcher(None, a, b, autojunk=False).ratio()


def get_text_matches(match_file):
  '''
  Given the path to a candidate match file, return a dictionary with
  keys for each matching text id, and values with form:
    [text_window_id, match_window_id]
  @args:
    str match_file: the path to a candidate match file
  @returns:
    dict: a dict with one key for each matching text id wherein the
      values for the given key have the format:
      [text_window_id, match_window_id]
  '''
  d = defaultdict(list)
  # candidate match files have the following structure
  #   file_id.window_id-match_file_id.match_window_id
  for i in read(match_file).split('#')[:-1]:
    file_ids, match_ids = i.split('-')
    file_id, file_segment_id = file_ids.split('.')
    match_id, match_segment_id = match_ids.split('.')
    d[match_id].append(( int(file_segment_id), int(match_segment_id) ))
  return d


def save_validated_matches(text_id, content, process_id):
  '''
  Save the validated matches to disk
  @args:
    str text_id: the id for an input text file. Text ids represent
      the index position of an input file within the list of input files
    str content: the formatted string of content to store for this
      text_id's validated matches
    int process_id: the index position of the current process in
      the list of all processes
  '''
  out_dir = os.path.join(config['tmp'], 'matches')
  make_dirs(out_dir)
  filename = text_id + '#' + str(process_id)
  if config['host_id']:
    filename += '#' + str(config['host_id'])
  append(os.path.join(out_dir, filename), content)


##
# 5) Cluster matches
##

def cluster_matches():
  '''
  Given the validated matches identified by Step 3, cluster neighboring
  matches so that matches from consecutive window ids will be combined
  into a single match object.
  '''
  validated_files = glob(os.path.join(config['tmp'], 'matches', '*'))
  args = [[i, c] for c, i in enumerate(get_host_args(validated_files))]
  for idx, _ in enumerate(multiprocess(cluster_file_matches, args)):
    print(' * clustered matches in', idx+1, 'of', len(validated_files), 'files')


def cluster_file_matches(args):
  '''
  Read in the path to the validated matches for an input file. Each
  validated match file contains matches in the following format:
    file_id.window_id-match_file_id.match_window_id|similarity#
  Each of these values represents a single validated match. Parse these
  matches into a dictionary with the format:
    d[match_file_id] = [(int(file_segment_id), int(match_segment_id), float(sim))]
  then format the matches and save in mongo.
  @args:
    list args:
      str: the path to a file with validated matches
      int: the index position of the current process among all
        processes in the current job on the current host
  '''
  validated_file, process_id = args
  text_id_a = int( os.path.basename(validated_file) )
  validated_matches = get_validated_matches(validated_file)
  for text_id_b in validated_matches:
    text_id_b = int(text_id_b)
    clusters = get_clusters(validated_matches[text_id_b])
    format_matches(text_id_a, text_id_b, clusters)


def get_validated_matches(validated_file_path):
  '''
  Given the path to a file with valided matches, return a dict with the form:
    dict[match_file_id] = [(int(file_segment_id), int(match_segment_id),
      float(similarity)]
  @args:
    str validated_file_path: the path to a validated file
  @returns:
    dict: a dictionary with the form described above
  '''
  d = defaultdict(list)
  for i in read(validated_file_path).split('#')[:-1]:
    ids, sim = i.split('|')
    file_ids, match_ids = ids.split('-')
    file_id, file_segment_id = [int(j) for j in file_ids.split('.')]
    match_file_id, match_segment_id = [int(j) for j in match_ids.split('.')]
    d[match_file_id].append(( file_segment_id, match_segment_id, float(sim) ))
  return d


def get_clusters(l):
  '''
  Given a list of three-element iterables (source_id, target_id, sim),
  find all sequences of values where s+1 and/or t+1 are matches.
  Using the sim values in each iterable, compute the mean sim for each
  passage cluster.
  @args:
    list l: a list of three-element values:
      (source_window_id, target_window_id, sim)
  @returns:
    [dict]: a list of dictionaries with the following keys:
      'a': a list of window_ids in text a
      'b': a list of window_ids in text b
      'sim': the mean similarity betwen all passages in this sequence
  '''
  a = [w[0] for w in l]
  b = [w[1] for w in l]
  d = nest(l)

  clusters = []
  for i in get_sequences(a):
    for j in get_sequences(b):
      cluster = {'a': [], 'b': [], 'sim': []}
      for ii in i:
        for jj in j:
          try:
            if not d[ii][jj]: continue
            cluster['a'].append(ii)
            cluster['b'].append(jj)
            cluster['sim'].append( d[ii][jj] )
          except KeyError:
            pass
      if cluster['a'] and cluster['b']:
        clusters.append({
          'a': sorted( list( set( cluster['a'] ) ) ),
          'b': sorted( list( set( cluster['b'] ) ) ),
          'sim': sum(cluster['sim']) / len(cluster['sim'])
        })
  return clusters


def nest(l):
  '''
  Given a list of iterables (int_a, int_b, float), return a dict with format:
    d[ int_a ][ int_b ] = sim
  @args:
    list l:
      a list of iterables with format (int, int, float)
  @returns:
    dict: a dictionary with format:
      d[ int_a ][ int_b ] = sim
  '''
  d = defaultdict(lambda: defaultdict())
  for i in l:
    d[ i[0] ][ i[1] ] = i[2]
  return d


def get_sequences(l):
  '''
  Given list `l`, return a list of lists where each sublist
  contains a maximally-long sequence of integers in l.
  @args:
    [int] l: a list of integers
  @returns:
    [[int]]: a list of lists of ints, where each sublist represents
      a maximally long sequence of integers in l.
  '''
  sequences = []
  for i in sorted( list( set(l) ) ):
    # check if each is 1 more than the last, as segment ids increment by 1
    if not sequences or sequences[-1][-1] != i-1:
      sequences.append([])
    sequences[-1].append(i)
  return sequences


##
# Format Matches
##

def format_matches(file_id_a, file_id_b, clusters):
  '''
  Given two file ids and `clusters` -- [{a: [1,2], b: [3,4,5]}, {}...]
  where a's values indicate a sequence of segment ids in text a that cluster
  with the sequence of text b's values -- format the matches into the required
  structure and save in mongo.
  @args:
    str file_id_a: a file id
    str file_id_b: a different file_id
    [dict] clusters: a list of dictionaries with form:
        [{a: [1,2], b: [3,4,5]}, {}...]
      in which each dictionary represents a cluster of matching passages
      between file_id_a and file_id_b
  '''
  text_id_to_path = {c: i for c, i in enumerate(infiles)}
  a_path = text_id_to_path[ int(file_id_a) ]
  b_path = text_id_to_path[ int(file_id_b) ]
  a_file = os.path.basename(a_path)
  b_file = os.path.basename(b_path)
  a_meta = metadata[a_file]
  b_meta = metadata[b_file]
  if not config['same_author_matches']:
    if a_meta.get('author', '') == b_meta.get('author', ''):
      return
  a_words = get_text_content(read(a_path, encoding=config['encoding'])).split()
  b_words = get_text_content(read(b_path, encoding=config['encoding'])).split()
  formatted = []
  for c in clusters:
    a_strings = get_match_strings(a_words, c['a'])
    b_strings = get_match_strings(b_words, c['b'])
    a_year = a_meta.get('year', '')
    b_year = b_meta.get('year', '')
    # identify the file published first as the 'source' file
    if (a_year and b_year) and (a_year < b_year):
      formatted.append({
        'similarity': c['sim'],
        'source_file_id': int(file_id_a),
        'target_file_id': int(file_id_b),
        'source_segment_ids': c['a'],
        'target_segment_ids': c['b'],
        'source_filename': a_file,
        'target_filename': b_file,
        'source_file_path': a_path,
        'target_file_path': b_path,
        'source_prematch': a_strings['prematch'],
        'target_prematch': b_strings['prematch'],
        'source_match': a_strings['match'],
        'target_match': b_strings['match'],
        'source_postmatch': a_strings['postmatch'],
        'target_postmatch': b_strings['postmatch'],
        'source_year': a_year,
        'target_year': b_year,
        'source_author': a_meta.get('author', ''),
        'target_author': b_meta.get('author', ''),
        'source_title': a_meta.get('title', ''),
        'target_title': b_meta.get('title', ''),
        'source_url': a_meta.get('url', ''),
        'target_url': b_meta.get('url', ''),
      })
    else:
      formatted.append({
        'similarity': c['sim'],
        'source_file_id': int(file_id_b),
        'target_file_id': int(file_id_a),
        'source_segment_ids': c['b'],
        'target_segment_ids': c['a'],
        'source_filename': b_file,
        'target_filename': a_file,
        'source_file_path': b_path,
        'target_file_path': a_path,
        'source_prematch': b_strings['prematch'],
        'target_prematch': a_strings['prematch'],
        'source_match': b_strings['match'],
        'target_match': a_strings['match'],
        'source_postmatch': b_strings['postmatch'],
        'target_postmatch': a_strings['postmatch'],
        'source_year': b_meta.get('year', ''),
        'target_year': a_meta.get('year', ''),
        'source_author': b_meta.get('author', ''),
        'target_author': a_meta.get('author', ''),
        'source_title': b_meta.get('title', ''),
        'target_title': a_meta.get('title', ''),
        'source_url': b_meta.get('url', ''),
        'target_url': a_meta.get('url', '')
      })
  save('matches', formatted)


def get_match_strings(words, window_ids):
  '''
  Format the prematch, match, and postmatch strings for a given match.
  @args:
    [str] words: a list of tokens
    [int] window_ids: a list of window id integers
  @returns:
    dict: a dictionary with the following keys:
      prematch: contains the string content leading up to a matched string
      match: contains the string content from an identified match
      postmatch: contains the string content after a matched string
  '''
  start = min(window_ids) * config['step']
  end = max(window_ids) * config['step'] + config['window_size']
  return {
    'prematch': ' '.join(words[max(0, start-config['window_size']):start]),
    'match': ' '.join(words[start:end]),
    'postmatch': ' '.join(words[end:end + config['window_size']])
  }


##
# Create database collections
##

def create_matches_collection():
  '''
  Read formatted, validated matches from disk and save to MongoDB.
  Should only be used if user previously selected to save results to disk.
  '''
  match_files = os.path.join(config['tmp'], 'results', 'matches', '*')
  for i in glob(match_files):
    print(' * saving match file', i)
    save('matches', load_json(i))


def create_typeahead_collection():
  '''
  Using the matches saved in mongo, create the typeahead collection.
  '''
  vals = []
  for i in ['source', 'target']:
    for j in ['author', 'title']:
      for k in db.matches.distinct(i + '_' + j):
        vals.append({'type': i + '_' + j, 'field': j, 'value': k})
  save('typeahead', vals)


def create_config_collection():
  '''
  Save the config options used to generate the current data to mongo.
  '''
  save('config', config)


def create_metadata_collection():
  '''
  Save the metadata for the infiles to mongo.
  '''
  vals = []
  for c, i in enumerate(infiles):
    vals.append({
      'filename': os.path.basename(i),
      'file_id': c,
      'path': i,
      'metadata': metadata.get(os.path.basename(i), {})
    })
  save('metadata', vals)


def create_scatterplot_collection():
  '''
  Precompute the mean and summed similarity values for each level
  of all factors used in the scatterplot visualization, including
  segment, title, and author values (with similarity values computed
  for those levels occuring before and after other levels)
  '''

  # create store for scatterplot data
  scatterplot_data = []

  for i in ['source', 'target']:
    for j in ['segment_ids', 'file_id', 'author']:
      for k in ['sum', 'mean']:
        data_nest = defaultdict(list)
        for l in list( db.matches.find({}) ):
          if j == 'segment_ids':
            level = i + '.' + str(l[i + '_file_id']) + '.'
            level += '.'.join( [str(m) for m in l[i + '_segment_ids']] )
          else:
            level = l[i + '_' + j]
          # ensure the level (aka data key) is a string
          if isinstance(level, list):
            level = '.'.join([str(i) for i in level])
          data_nest[level].append(l)

        for level in data_nest:
          sims = [o['similarity'] for o in data_nest[level]]
          sim = sum(sims) if k == 'sum' else sum(sims) / len(sims)
          o = data_nest[level][0]
          scatterplot_data.append({
            'type': i,
            'unit': j,
            'statistic': k,
            'key': level,
            'similarity': sim,
            'title': o[i + '_title'],
            'author': o[i + '_author'],
            'match': o[i + '_match'],
            'source_year': o['source_year'],
            'target_year': o['target_year'],
          })
  save('scatterplot', scatterplot_data)


def create_collections():
  '''
  Populate the required mongo data tables. If this process is running
  in a multhost environment, only allow one host to create the mongo
  collections.
  '''
  # in a multihost environment, only allow one host to save to mongo
  if config['multihost'] and config['host_id'] != 1:
    return
  # load matches from disk before building other collections, if relevant
  if config['load_matches']:
    create_matches_collection()
  # build the remainder of the collections given match data
  create_typeahead_collection()
  create_config_collection()
  create_metadata_collection()
  create_scatterplot_collection()


##
# Config Helpers
##

def get_config():
  defaults = {
    'max_cores': max(cpu_count()-2, 1),
    'encoding': 'utf8',
    'xml_tag': False,
    'step': 4,
    'window_size': 14,
    'n_permutations': 256,
    'hashband_length': 4,
    'min_similarity': 0.65,
    'same_author_matches': True,
    'mongo_host': 'localhost',
    'mongo_port': 27017,
    'save_to': 'mongo',
    'db': 'intertext',
    'tmp': 'tmp',
    'clear_tmp_files': True,
    'clear_db': True,
    'host_id': args.host_id,
    'host_count': args.host_count,
    'multihost': args.host_id and args.host_count,
    'load_matches': args.load_matches,
  }
  config = load_json('config.json')
  for k in defaults:
    if not k in config:
      config[k] = defaults[k]

  # if loading matches from disk, ensure we're saving to mongo
  if config['load_matches']:
    config['save_to'] = 'mongo'
  return config


##
# Main
##

def main():
  '''
  Clear tmp files, clear db, then load results or generate new results.
  '''
  if config['clear_tmp_files']:
    print(' * clearing tmp files')
    rm_dirs(config['tmp'])

  # remove saved records in mongo
  if config['clear_db']:
    print(' * clearing db')
    [db[c].drop() for c in db.collection_names()]

  # if loading results, only create collections and exit
  if config['load_matches']:
    create_collections()
    return

  # run all processing
  tmp = config['tmp']
  run_task(hash_inputs, None)
  run_task(sort_hashbands, os.path.join(tmp, 'sorted', '*', '*'))
  run_task(find_candidate_matches, os.path.join(tmp, 'candidates'))
  run_task(validate_matches, os.path.join(tmp, 'matches'))
  run_task(cluster_matches, None)
  create_collections()


if __name__ == '__main__':

  # configure cli parser
  parser = argparse.ArgumentParser(description='Discover text reuse')
  parser.add_argument('-host_id', default=None, type=int, help='Current host id')
  parser.add_argument('-host_count', default=None, type=int, help='Total host count')
  parser.add_argument('-load_matches', default=False, type=bool, help='Load results from disk?')
  args = parser.parse_args()

  # load config and globals
  config = get_config()
  infiles = glob(config['infiles'])
  text_ids = [str(i) for i in range(len(infiles))]
  process_ids = range(config['max_cores'])
  metadata = get_metadata()
  db = get_db()

  # validate inputs are present
  if not infiles: raise Exception('No input files were found!')

  # start the main process
  main()
