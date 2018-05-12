'''
Read in a list of infiles and begin composing tasks
for celery consumers. On a single host machine (e.g.
a laptop) celery will distribute the work over all
cores. In a multihost setup, celery will allow all hosts
that receive the worker command to join the worker party.
'''

from __future__ import division, print_function, absolute_import
from collections import defaultdict
from datasketch import MinHash
from itertools import combinations
from os.path import join, basename
from difflib import SequenceMatcher
from celery import Celery
from nltk import ngrams
from bs4 import BeautifulSoup
from glob import glob
import os
import sys
import intertext.helpers as helpers

##
# Globals
##

# load config and globals
config = helpers.config
infiles = helpers.infiles
text_ids = helpers.text_ids
metadata = helpers.get_metadata()
db = helpers.get_db()

# validate inputs are present
if not infiles: raise Exception('No input files were found!')

app = Celery('tasks',
  backend=config['redis_url'],
  broker=config['redis_url'])

##
# 0) Manage disk state
##

@app.task(soft_time_limit=60)
def combine_files_in_dir(_dir):
  '''
  Given a dir, combine all files in that dir that share an identical file root
  where root is defined by the string up to the first #
  '''
  for file in glob( join(_dir, '*#*') ):
    file_name = basename(file).split('#')[0]
    helpers.write(join(_dir, file_name), helpers.read(file))
    os.remove(file)


##
# 1) Hash input texts
##

@app.task(soft_time_limit=60)
def hash_input_file(text_id, process_id):
  '''
  Write a list of hashbands to disk for an infile.
  @args:
    str text_id: the text_id of an infile, where each text_id is
      an integer cast as a string that represents the index
      position of an infile within the list of all infiles
    str process_id: an integer cast as a string that represents
      the index position of a process within all processes
      allocated for this job among all hosts
  '''
  d = defaultdict(lambda: defaultdict(list))
  text_path = infiles[int(text_id)]
  for window_id, window in enumerate(get_windows(text_path)):
    for hashband in get_hashbands(window):
      # a and b represent the letters 0+1 and 2+3 in hashband
      # these are used to create a simple tree structure on disk
      # for quick hashband retrieval
      a, b = hashband[0:2], hashband[2:4]
      d[a][b].append(hashband + '-' + text_id + '.' + str(window_id))
  # concate writes for each hashband
  for a in d.keys():
    for b in d[a].keys():
      out_dir = os.path.join(config['tmp'], 'hashbands', a, b)
      out_path = os.path.join(out_dir, a + b + '#' + str(process_id))
      helpers.make_dirs(out_dir)
      helpers.write(out_path, '#'.join(d[a][b]) + '#')


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
    return helpers.read(path, encoding=config['encoding'])
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
# 2) Find candidate matches
##

@app.task(soft_time_limit=60)
def find_candidates(_dir, process_id):
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
    str: the path to a directory that is a child directory of
      /tmp/hashbands
    str: an integer cast as a string that represents
      the index position of a process within all processes
      allocated for this job among all hosts
  '''
  d = defaultdict(list)
  count = 0
  for file in glob(join(_dir, '*', '*')):
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
  for i in helpers.read(file_path).split('#')[:-1]:
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
    outdir = join(config['tmp'], 'candidates')
    helpers.make_dirs(outdir)
    filename = file_id + '#' + str(process_id)
    helpers.write(join(outdir, filename), '#'.join(d[file_id]) + '#')


##
# 3) Validate candidate matches
##

@app.task(soft_time_limit=60)
def validate_matches(match_file):
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
    str: the path to a candidate match file
  '''
  validated = ''
  text_matches = get_text_matches(match_file)
  text_id = basename(match_file)
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
  save_validated_matches(text_id, validated)


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
  for i in helpers.read(match_file).split('#')[:-1]:
    file_ids, match_ids = i.split('-')
    file_id, file_segment_id = file_ids.split('.')
    match_id, match_segment_id = match_ids.split('.')
    d[match_id].append(( int(file_segment_id), int(match_segment_id) ))
  return d


def save_validated_matches(text_id, content):
  '''
  Save the validated matches to disk
  @args:
    str text_id: the id for an input text file. Text ids represent
      the index position of an input file within the list of input files
    str content: the formatted string of content to store for this
      text_id's validated matches
  '''
  out_dir = join(config['tmp'], 'matches')
  helpers.make_dirs(out_dir)
  helpers.write(join(out_dir, text_id), content)


##
# 4) Cluster matches
##

@app.task(soft_time_limit=60)
def cluster_matches(validated_file):
  '''
  Read in the path to the validated matches for an input file. Each
  validated match file contains matches in the following format:
    file_id.window_id-match_file_id.match_window_id|similarity#
  Each of these values represents a single validated match. Parse these
  matches into a dictionary with the format:
    d[match_file_id] = [(int(file_segment_id), int(match_segment_id), float(sim))]
  then format the matches and save in mongo.
  @args:
    str: the path to a file with validated matches
  '''
  text_id_a = int( basename(validated_file) )
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
  for i in helpers.read(validated_file_path).split('#')[:-1]:
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

def format_matches(file_id_one, file_id_two, clusters):
  '''
  Given two file ids and `clusters` -- [{a: [1,2], b: [3,4,5]}, {}...]
  where a's values indicate a sequence of segment ids in text a that
  cluster with the sequence of text b's values -- format the matches
  into the required structure and save in mongo.
  @args:
    str file_id_a: a file id
    str file_id_b: a different file_id
    [dict] clusters: a list of dictionaries with form:
        [{a: [1,2], b: [3,4,5]}, {}...]
      in which each dictionary represents a cluster of matching
      passages between file_id_a and file_id_b
  '''
  text_id_to_path = {c: i for c, i in enumerate(infiles)}

  # sort the files by date so a occurs first historically
  one_meta = metadata[ basename( text_id_to_path[int(file_id_one)] ) ]
  two_meta = metadata[ basename( text_id_to_path[int(file_id_two)] ) ]
  if one_meta.get('year', '') < two_meta.get('year', ''):
    file_id_a = file_id_one
    file_id_b = file_id_two
  else:
    file_id_a = file_id_two
    file_id_b = file_id_one

  # pluck out the required attributes for the two file ids
  a_path = text_id_to_path[ int(file_id_a) ]
  b_path = text_id_to_path[ int(file_id_b) ]
  a_file = basename(a_path)
  b_file = basename(b_path)
  a_meta = metadata[a_file]
  b_meta = metadata[b_file]
  if not config['same_author_matches']:
    if a_meta.get('author', '') == b_meta.get('author', ''):
      return
  a_words = get_text_content(helpers.read(a_path, encoding=config['encoding'])).split()
  b_words = get_text_content(helpers.read(b_path, encoding=config['encoding'])).split()
  formatted = []
  for c in clusters:
    a_strings = get_match_strings(a_words, c['a'])
    b_strings = get_match_strings(b_words, c['b'])
    a_year = a_meta.get('year', '')
    b_year = b_meta.get('year', '')

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

  helpers.save('matches', formatted)


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
  match_files = join(config['tmp'], 'results', 'matches', '*')
  for i in glob(match_files):
    print(' * saving match file', i)
    helpers.save('matches', helpers.load_json(i))


def create_typeahead_collection():
  '''
  Using the matches saved in mongo, create the typeahead collection.
  '''
  vals = []
  for i in ['source', 'target']:
    for j in ['author', 'title']:
      for k in db.matches.distinct(i + '_' + j):
        vals.append({'type': i + '_' + j, 'field': j, 'value': k})
  helpers.save('typeahead', vals)


def create_config_collection():
  '''
  Save the config options used to generate the current data to mongo.
  '''
  helpers.save('config', config)


def create_metadata_collection():
  '''
  Save the metadata for the infiles to mongo.
  '''
  vals = []
  for c, i in enumerate(infiles):
    vals.append({
      'filename': basename(i),
      'file_id': c,
      'path': i,
      'metadata': metadata.get(basename(i), {})
    })
  helpers.save('metadata', vals)


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
  helpers.save('scatterplot', scatterplot_data)


def create_collections():
  '''
  Populate the required mongo data tables. If this process is running
  in a multhost environment, only allow one host to create the mongo
  collections.
  '''
  create_typeahead_collection()
  create_config_collection()
  create_metadata_collection()
  create_scatterplot_collection()
