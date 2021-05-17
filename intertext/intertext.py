from networkx.algorithms.components.connected import connected_components
from vectorizedMinHash import VectorizedMinHash,fastNGramHashes
from collections import defaultdict, Hashable, Counter
from datasketch import MinHash, MinHashLSH
from difflib import SequenceMatcher
from itertools import combinations
from unidecode import unidecode
from contextlib import closing
from bs4 import BeautifulSoup
from Levenshtein import ratio
from copy import deepcopy
from nltk import ngrams
import multiprocessing
import numpy as np
import functools
import distutils
import requests
import argparse
import networkx
import sqlite3
import zipfile
import codecs
import shutil
import time
import uuid
import glob
import json
import os


try:
  import cupy
  CUDA_AVAILABLE = True
except:
  CUDA_AVAILABLE = False


# global config
config = {
  'infile_glob': '',
  'banish_glob': '',
  'output': 'output',
  'metadata': {},
  'encoding': 'utf8',
  'xml_base_tag': None,
  'xml_remove_tags': tuple(),
  'window_length': 14,
  'hashband_length': 4,
  'hashband_step': 3,
  'batch_size': 10**5,
  'write_frequency': 10**5,
  'slide_length': 4,
  'chargram_length': 4,
  'threshold': 0.5,
  'min_sim': 50,
  'banish_distance': 4,
  'max_file_sim': None,
  'client': '0.0.1a',
  'in_memory': False,
  'update_client': False,
  'strip_diacritics': False,
  'verbose': False,
  'db': 'disk',
}


'''
TODO:
  * add flag to indicate if same-author matches are allowed
  * add support for CSV metadata
  * add support for xml + txt in same run
  * throw an error if the disk location is a networked drive
  * make db swapable for MySQL
'''


# path globals
source_location = os.path.dirname(os.path.realpath(__file__))
client_location = os.path.join(source_location, 'client')
cache_location = os.path.join(source_location, 'cache')


# db globals
row_delimiter = '\n'
field_delimiter = '-'


# minhashing
hasher = VectorizedMinHash(n_perm=256, mirror=True)


def parse():
  '''Parse the command line arguments and initialize text processing'''
  description = 'Discover and visualize text reuse'
  parser = argparse.ArgumentParser(description=description, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--infiles', '-i', type=str, default=config['infile_glob'], dest='infile_glob', help='path to a glob of text files to process', required=False)
  parser.add_argument('--banish', '-b', type=str, default=config['banish_glob'], dest='banish_glob', help='path to a glob of text files to remove from matches', required=False)
  parser.add_argument('--metadata', '-m', type=str, default=config['metadata'], help='path to a JSON metadata file (see README)', required=False)
  parser.add_argument('--encoding', '-e', type=str, default=config['encoding'], help='the encoding of infiles', required=False)
  parser.add_argument('--window_length', '-w', type=int, default=config['window_length'], help='the length of windows when processing files (see README)', required=False)
  parser.add_argument('--hashband_length', '-hb', type=int, default=config['hashband_length'], help='the number of minhash values per hashband', required=False)
  parser.add_argument('--hashband_step', '-hs', type=int, default=config['hashband_step'], help='the number of minhash units to slide hashband windows', required=False)
  parser.add_argument('--chargram_length', '-cl', type=int, default=config['chargram_length'], help='the number of characters per character shingle', required=False)
  parser.add_argument('--write_frequency', '-wf', type=int, default=config['write_frequency'], help='the max number of write operations to store in RAM')
  parser.add_argument('--slide_length', '-l', type=int, default=config['slide_length'], help='the length to slide windows when processing files (see README)', required=False)
  parser.add_argument('--threshold', '-t', type=int, default=config['threshold'], help='the minhash threshold value (see README)', required=False)
  parser.add_argument('--min_sim', '-s', type=int, default=config['min_sim'], help='the minimum similarity of matches to retain)', required=False)
  parser.add_argument('--banish_distance', '-bd', type=int, default=config['banish_distance'], help='the graph distance to travel when banishing linked matches', required=False)
  parser.add_argument('--max_file_sim', type=int, default=config['max_file_sim'], help='the maximum similarity between two files such that matches are retained', required=False)
  parser.add_argument('--output', '-o', type=str, default=config['output'], help='the output location', required=False)
  parser.add_argument('--client', '-c', type=str, default=config['client'], help='the client version to fetch and display', required=False)
  parser.add_argument('--xml_base_tag', type=str, default=config['xml_base_tag'], help='if specified, text within this parent tag will be parsed', required=False)
  parser.add_argument('--xml_remove_tags', type=tuple, default=config['xml_remove_tags'], help='if specified, text within these tags will be removed', required=False)
  parser.add_argument('--strip_diacritics', default=config['strip_diacritics'], help='if specified, diacritics will be parsed from texts during processing', required=False, action='store_true')
  parser.add_argument('--update_client', default=config['update_client'], help='boolean indicating whether to update the stored client', required=False, action='store_true')
  parser.add_argument('--verbose', '-v', default=config['verbose'], help='if specified, the intertext process will log more operations', required=False, action='store_true')
  parser.add_argument('--db', default=config['db'], help='specify sqlite to use a sqlite db', required=False)
  config.update(vars(parser.parse_args()))
  if config['update_client']: remove_client(**config)
  download_client(**config)
  if config.get('infile_glob'): process_texts(**config)


def remove_client(**kwargs):
  '''Remove the cached client so it will be fetched afresh'''
  print(' * clearing cached client')
  if os.path.exists(os.path.join(source_location, 'client')):
    shutil.rmtree(client_location)


def download_client(**kwargs):
  '''Download the client to the cache (if necessary) and copy to the output directory'''
  if not os.path.exists(client_location):
    print(f' * fetching client version {kwargs["client"]}')
    os.makedirs(client_location)
    zip_location = os.path.join(client_location, 'client.zip')
    # download the zip archive
    with open(zip_location, 'wb') as out:
      url = f'https://lab-apps.s3-us-west-2.amazonaws.com/intertext-builds/intertext-client-{kwargs["client"]}.zip'
      out.write(requests.get(url).content)
    # extract the zip archive
    with zipfile.ZipFile(zip_location, 'r') as z:
      z.extractall(client_location)
    # remove extant matches if user provided inputs
    if kwargs.get('infile_glob'):
      former_api_location = os.path.join(client_location, 'build', 'api')
      if os.path.exists(former_api_location):
        shutil.rmtree(former_api_location)
  # copy the `build` directory to the output directory
  if os.path.exists(kwargs['output']):
    shutil.rmtree(kwargs['output'])
  # copy the web client
  shutil.copytree(os.path.join(client_location, 'build'), kwargs['output'])


def process_texts(**kwargs):
  '''Process the user's texts using the specified params'''

  # typecheck inputs
  assert kwargs['min_sim'] >= 1 and kwargs['min_sim'] <= 100

  # identify the infiles
  infiles = sorted(glob.glob(kwargs['infile_glob']))
  if len(infiles) == 0:
    raise Exception('No infiles could be found!')

  # identify banished files and add to infiles
  if kwargs['banish_glob']:
    banished_files = sorted(glob.glob(kwargs['banish_glob']))
    infiles += banished_files
    banished_file_set = set(banished_files)
    banished_file_ids = set()
    for file_idx, file in enumerate(kwargs['infiles']):
      if file in banished_file_set:
        banished_file_ids.add(file_idx)
    kwargs['banished_file_ids'] = banished_file_ids
  kwargs['infiles'] = infiles

  # if the user provided metadata, store it in the kwargs
  if kwargs.get('metadata'):
    kwargs['metadata'] = json.load(open(kwargs['metadata']))
  else:
    kwargs['metadata'] = {
      os.path.basename(i): {
        'author': 'Unknown',
        'title': os.path.basename(i),
      } for i in kwargs['infiles']
    }

  # create directories
  for i in ['matches', 'scatterplots', 'indices']:
    path = os.path.join(kwargs['output'], 'api', i)
    if not os.path.exists(path):
      os.makedirs(path)

  for i in ['minhashes']:
    path = os.path.join(cache_location, i)
    if not os.path.exists(path):
      os.makedirs(path)

  for i in range(len(kwargs['infiles'])):
    path = os.path.join(kwargs['output'], 'api', 'matches', str(i))
    if not os.path.exists(path):
      os.makedirs(path)

  # save JSON with the list of infiles
  with open(os.path.join(kwargs['output'], 'api', 'files.json'), 'w') as out:
    json.dump(kwargs['infiles'], out)

  # create the db
  initialize_db(**kwargs)

  # minhash files & store hashbands in db
  print(' * creating minhashes')
  get_all_hashbands(**kwargs)

  # find all hashbands that have multiple distict file_ids
  print(' * identifying match candidates')
  get_all_match_candidates(**kwargs)

  # validate matches from among the candidates
  print(' * validating matches')
  validate_all_matches(**kwargs)

  # format matches into JSON for client consumption
  print(' * formatting matches')
  format_all_matches(**kwargs)

  # combine all matches into a single match object
  print(' * formatting JSON outputs')
  create_all_match_json(**kwargs)


##
# Minhash files
##


def get_all_hashbands(**kwargs):
  '''Generate and save hashbands for each infile'''
  pool = multiprocessing.Pool()
  l = [[idx, i] for idx, i in enumerate(kwargs['infiles'])]
  f = functools.partial(get_file_hashbands, **kwargs)
  for i in pool.map(f, l): pass
  pool.close()
  pool.join()


def get_file_hashbands(args, **kwargs):
  '''Minhash a file and save [[hashband, file_idx, window_idx]]'''
  file_idx, file_path = args
  minhashes = get_file_minhashes(file_path, **kwargs)
  # get the hashbands for this minhash
  hashbands = []
  for window_idx, minhash in enumerate(minhashes):
    for hdx, h in enumerate(ngrams(minhash, kwargs['hashband_length'])):
      if hdx % kwargs['hashband_step'] == 0:
        hashbands.append(['.'.join([str(i) for i in h]), file_idx, window_idx])
  write_hashbands(hashbands, **kwargs)


def get_file_minhashes(file_path, **kwargs):
  '''Return the minhash array for a file'''
  minhash_path = os.path.join(cache_location, 'minhashes', os.path.basename(file_path) + '.npy')
  if os.path.exists(minhash_path):
    return np.load(minhash_path)
  # run minhash algorithm on file
  l = []
  for window_idx, window in enumerate(get_windows(file_path, **get_cacheable(kwargs))):
    char_hashes = fastNGramHashes(window.lower().encode(kwargs['encoding']), n=kwargs['chargram_length'])
    fingerprint = hasher.fingerprint(char_hashes, cuda=CUDA_AVAILABLE)
    l.append(fingerprint)
  minhashes = np.array(l)
  # save the minhashes array unless running in memory mode
  if not kwargs['in_memory']:
    np.save(minhash_path, minhashes)
  return minhashes


##
# Get match candidates
##


def get_all_match_candidates(**kwargs):
  '''Find all hashbands that have multiple distinct file_ids and save as match candidates'''
  rows = []
  for row in stream_hashbands(**kwargs):
    rows.append(row)
    # the hashbands table is our largest data artifact - paginate in blocks
    if len(rows) >= kwargs['batch_size']:
      process_candidate_hashbands(rows, **kwargs)
      rows = []
  process_candidate_hashbands(rows, **kwargs)


def process_candidate_hashbands(l, **kwargs):
  '''Given a set of hashbands, subdivide into processes to find match candidates for each'''
  if kwargs['verbose']:
    print(' * processing match candidate block')
  pool = multiprocessing.Pool()
  l = list(subdivide(l, len(l) // 1)) # multiprocessing.cpu_count()
  f = functools.partial(get_hashband_match_candidates, **kwargs)
  writes = set()
  for idx, i in enumerate(pool.map(f, l)):
    writes.update(i)
    if len(writes) >= kwargs['write_frequency'] or idx == len(l)-1:
      write_candidates(writes, **kwargs)
      writes = set()
  if writes: write_candidates(writes, **kwargs)
  pool.close()
  pool.join()


def get_hashband_match_candidates(args, **kwargs):
  '''Given a hashband, save the file_id, window_id values that contain the hashband'''
  results = []
  last_hashband = args[0][0]
  hashband_values = set()
  for idx, i in enumerate(args):
    hashband, file_id, window_id = i
    tup = tuple([file_id, window_id])
    if hashband == last_hashband:
      hashband_values.add(tup)
    elif (hashband != last_hashband) or (idx == len(args)-1):
      for a, b in combinations(hashband_values, 2):
        # skip same file matches
        if a[0] == b[0]:
          continue
        elif a[0] < b[0]:
          results.append(tuple([a[0], b[0], a[1], b[1]]))
        else:
          results.append(tuple([b[0], a[0], b[1], a[1]]))
      hashband_values = set([tup])
    last_hashband = hashband
  return set(results)


def subdivide(l, n):
  '''Subdivide list `l` into units `n` long'''
  if not l or not n: return l
  for i in range(0, len(l), n):
    yield l[i:i + n]


##
# Validate matches
##


def validate_all_matches(**kwargs):
  '''Run match validations and yield [a_file,b_file,a_window,b_window]'''
  pool = multiprocessing.Pool()
  l = stream_candidate_file_id_pairs(**kwargs)
  f = functools.partial(validate_file_matches, **kwargs)
  for i in pool.map(f, l): pass
  pool.close()
  pool.join()


def validate_file_matches(args, **kwargs):
  '''Validate the matches for a single file pair and return [a_file,b_file,a_window,b_window]'''
  file_id_a, file_id_b = args
  matches = []
  for i in stream_matching_candidate_windows(file_id_a, file_id_b, **kwargs):
    file_id_a, file_id_b, window_id_a, window_id_b = i
    file_a_windows = list(get_windows(kwargs['infiles'][file_id_a], **get_cacheable(kwargs)))
    file_b_windows = list(get_windows(kwargs['infiles'][file_id_b], **get_cacheable(kwargs)))
    text_a = file_a_windows[window_id_a]
    text_b = file_b_windows[window_id_b]
    sim = ratio(text_a, text_b) * 100
    if sim > (kwargs['min_sim'] * 0.85):
      sim = SequenceMatcher(None, text_a, text_b, autojunk=False).ratio() * 100
      if sim >= kwargs['min_sim']:
        matches.append([file_id_a, file_id_b, window_id_a, window_id_b, int(sim)])
  write_matches(matches, **kwargs)


##
# Format matches
##


def format_all_matches( **kwargs):
  '''Format the match objects for each infile and store as JSON'''
  pool = multiprocessing.Pool()
  l = stream_matching_file_id_pairs(**kwargs)
  f = functools.partial(format_file_matches, **kwargs)
  for i in pool.map(f, l): pass
  pool.close()
  pool.join()


def format_file_matches(args, **kwargs):
  ''''Format the matches for a single file pair'''
  file_id_a, file_id_b = args
  l = stream_file_pair_matches(file_id_a, file_id_b, **kwargs)
  # check to see if this file pair has >= max allowed similarity
  a_windows = get_windows(kwargs['infiles'][file_id_a], **get_cacheable(kwargs))
  b_windows = get_windows(kwargs['infiles'][file_id_b], **get_cacheable(kwargs))
  if kwargs['max_file_sim']:
    if (len(l) > len(a_windows) * kwargs['max_file_sim']) or \
       (len(l) > len(b_windows) * kwargs['max_file_sim']):
      print(' * file pair', *args, 'has >= max_file_sim; skipping!')
      return []
  # cluster the matches so sequential matching windows are grouped into a single match
  clusters = []
  _, _, window_a, window_b, sims = zip(*l)
  d = defaultdict(lambda: defaultdict())
  for a, b, sim in zip(window_a, window_b, sims):
    d[a][b] = sim
  for a in get_sequences(window_a):
    for b in get_sequences(window_b):
      cluster = {'a': set(), 'b': set(), 'sim': []}
      for a_i in a:
        for b_i in b:
          if d.get(a_i, {}).get(b_i):
            cluster['a'].add(a_i)
            cluster['b'].add(b_i)
            cluster['sim'].append(d[a_i][b_i])
      if cluster['a'] and cluster['b']:
        sim = int(sum(cluster['sim']) / len(cluster['sim']))
        if sim < kwargs['min_sim']: continue
        clusters.append({
          'a': sorted(cluster['a']),
          'b': sorted(cluster['b']),
          'sim': sim,
        })
  # format the matches, then save into both file_id_a and file_id_b directories
  formatted = format_matches(file_id_a, file_id_b, clusters, **kwargs)
  for i in [file_id_a, file_id_b]:
    out_dir = os.path.join(kwargs['output'], 'api', 'matches', str(i))
    with open(os.path.join(out_dir, f'{file_id_a}-{file_id_b}.json'), 'w') as out:
      json.dump(formatted, out)


def format_matches(file_id_a, file_id_b, clusters, **kwargs):
  '''Given integer file ids and clusters [{a: [], b: [], sim: []}] format matches for display'''
  file_id_a, file_id_b, clusters = order_match_pair(file_id_a, file_id_b, clusters, **kwargs)
  a_meta = kwargs.get('metadata', {}).get(os.path.basename(kwargs['infiles'][file_id_a]), {})
  b_meta = kwargs.get('metadata', {}).get(os.path.basename(kwargs['infiles'][file_id_b]), {})
  # format the matches
  a_words = get_words(kwargs['infiles'][file_id_a], **get_cacheable(kwargs, {'display': True}))
  b_words = get_words(kwargs['infiles'][file_id_b], **get_cacheable(kwargs, {'display': True}))
  formatted = []
  for c in clusters:
    a_strings = get_match_strings(a_words, c['a'], **get_cacheable(kwargs))
    b_strings = get_match_strings(b_words, c['b'], **get_cacheable(kwargs))
    formatted.append({
      '_id': str(uuid.uuid4()),
      'similarity': c['sim'],
      'source_file_id': int(file_id_a),
      'target_file_id': int(file_id_b),
      'source_segment_ids': c['a'],
      'target_segment_ids': c['b'],
      'source_filename': os.path.basename(kwargs['infiles'][file_id_a]),
      'target_filename': os.path.basename(kwargs['infiles'][file_id_b]),
      'source_file_path': kwargs['infiles'][file_id_a],
      'target_file_path': kwargs['infiles'][file_id_b],
      'source_prematch': a_strings['prematch'],
      'target_prematch': b_strings['prematch'],
      'source_match': a_strings['match'],
      'target_match': b_strings['match'],
      'source_postmatch': a_strings['postmatch'],
      'target_postmatch': b_strings['postmatch'],
      'source_year': str(a_meta.get('year', '')),
      'target_year': str(b_meta.get('year', '')),
      'source_author': a_meta.get('author', ''),
      'target_author': b_meta.get('author', ''),
      'source_title': a_meta.get('title', ''),
      'target_title': b_meta.get('title', ''),
      'source_url': a_meta.get('url', ''),
      'target_url': b_meta.get('url', ''),
    })
  return(formatted)


def order_match_pair(file_id_a, file_id_b, clusters, **kwargs):
  '''Set file id a to the previously published file (if relevant)'''
  a_meta = kwargs.get('metadata', {}).get(os.path.basename(kwargs['infiles'][file_id_a]), {})
  b_meta = kwargs.get('metadata', {}).get(os.path.basename(kwargs['infiles'][file_id_b]), {})
  if a_meta and \
     b_meta and \
     a_meta.get('year') and \
     b_meta.get('year') and \
     b_meta.get('year') < a_meta.get('year'):
    return [
      file_id_b,
      file_id_a,
      [{'a': c['b'], 'b': c['a'], 'sim': c['sim']} for c in deepcopy(clusters)]
    ]
  return [
    file_id_a,
    file_id_b,
    clusters,
  ]


def get_match_strings(words, window_ids, **kwargs):
  '''Given a list of words and window ids, format prematch, match, and postmatch strings for a match'''
  start = min(window_ids) * kwargs['slide_length']
  end = max(window_ids) * kwargs['slide_length'] + kwargs['window_length']
  return {
    'prematch': ' '.join(words[max(0, start-kwargs['window_length']):start]),
    'match': ' '.join(words[start:end]),
    'postmatch': ' '.join(words[end:end + kwargs['window_length']]),
  }


def get_sequences(l):
  '''Given list of ints `l`, return [[integer sequence in l], [integer sequence in l]]'''
  sequences = []
  for i in sorted( list( set(l) ) ):
    # check if each is 1 more than the last, as segment ids increment by 1
    if not sequences or sequences[-1][-1] != i-1:
      sequences.append([])
    sequences[-1].append(i)
  return sequences


##
# Create output JSON
##


def create_all_match_json(**kwargs):
  '''Create the output JSON to be consumed by the web client'''
  # combine all the matches in each match directory into a composite match file
  guid_to_int = defaultdict(lambda: len(guid_to_int))
  for match_directory in glob.glob(os.path.join(kwargs['output'], 'api', 'matches', '*')):
    # l contains the flat list of matches for a single input file
    l = []
    for j in glob.glob(os.path.join(match_directory, '*')):
      with open(j) as f:
        l += json.load(f)
    for i in l:
      i['_id'] = guid_to_int[i['_id']]
    with open(os.path.join(match_directory + '.json'), 'w') as out:
      json.dump(l, out)
    shutil.rmtree(match_directory)

  # map each author and title to the files in which that string occurs and save those maps
  authors = [kwargs['metadata'].get(os.path.basename(i), {}).get('author', 'Unknown') for i in kwargs['infiles']]
  titles = [kwargs['metadata'].get(os.path.basename(i), {}).get('title', os.path.basename(i)) for i in kwargs['infiles']]
  author_d = defaultdict(list)
  title_d = defaultdict(list)
  for idx, i in enumerate(authors): author_d[i].append(idx)
  for idx, i in enumerate(titles): title_d[i].append(idx)
  with open(os.path.join(kwargs['output'], 'api', 'authors.json'), 'w') as out:
    json.dump(author_d, out)
  with open(os.path.join(kwargs['output'], 'api', 'titles.json'), 'w')  as out:
    json.dump(title_d, out)

  # create minimal representations of all matches to be sorted by each sort heuristic below
  l = []
  for file_id, matches in stream_match_lists(**kwargs):
    for match_idx, match in enumerate(matches):
      l.append([
        file_id,
        match.get('_id'),
        match_idx,
        int(file_id) == int(match.get('source_file_id')),
        match.get('similarity', ''),
        match.get('source_author' ''),
        match.get('source_title', ''),
        match.get('source_year', ''),
      ])

  # create and store the file_id.match_index indices for each sort heuristic
  for label, idx in [['similarity', -4], ['author', -3], ['title', -2], ['year', -1]]:
    ids = [[
      int(i[0]),
      int(i[1]),
      int(i[2]),
      bool(i[3]),
      int(i[4]),
    ] for i in sorted(l, key=lambda j: j[idx])]
    with open(os.path.join(kwargs['output'], 'api', 'indices', 'match-ids-by-{}.json'.format(label)), 'w') as out:
      json.dump(ids, out)

  # create the scatterplot data
  write_scatterplots(**kwargs)


def write_scatterplots(**kwargs):
  '''Write the scatterplot JSON'''
  out_dir = os.path.join(kwargs['output'], 'api', 'scatterplots')
  for i in ['source', 'target']:
    for j in ['segment_ids', 'file_id', 'author']:
      for k in ['sum', 'mean']:
        data_nest = defaultdict(list)
        for file_id, matches in stream_match_lists(**kwargs):
          for match in matches:
            if j == 'segment_ids':
              level = i + '.' + str(match[i + '_file_id']) + '.'
              level += '.'.join( [str(m) for m in match[i + '_segment_ids']] )
            else:
              level = match[i + '_' + j]
            # ensure the level (aka data key) is a string
            if isinstance(level, list):
              level = '.'.join([str(i) for i in level])
            data_nest[level].append(match)
        # format the scatterplot data
        scatterplot_data = []
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
        # write the scatterplot data
        with open(os.path.join(out_dir, '{}-{}-{}.json'.format(i, j, k)), 'w') as out:
          json.dump(scatterplot_data, out)


##
# DB
##


def initialize_db(**kwargs):
  '''Run all setup steps to create the database'''
  if kwargs.get('db') == 'sqlite':
    with closing(get_db(initialize=True, **kwargs)) as db:
      cursor = db.cursor()
      cursor.execute('DROP TABLE IF EXISTS hashbands;')
      cursor.execute('DROP TABLE IF EXISTS candidates;')
      cursor.execute('DROP TABLE IF EXISTS matches;')
      cursor.execute('CREATE TABLE hashbands (hashband TEXT, file_id INTEGER, window_id INTEGER);')
      cursor.execute('CREATE TABLE candidates (file_id_a INTEGER, file_id_b INTEGER, window_id_a INTEGER, window_id_b INTEGER, UNIQUE(file_id_a, file_id_b, window_id_a, window_id_b));')
      cursor.execute('CREATE TABLE matches (file_id_a INTEGER, file_id_b INTEGER, window_id_a INTEGER, window_id_b INTEGER, similarity INTEGER);')
  else:
    for i in ['hashbands', 'candidates', 'matches']:
      path = os.path.join('db', i)
      if not os.path.exists(path):
        os.makedirs(path)


def get_db(initialize=False, **kwargs):
  '''Return a Sqlite DB'''
  if kwargs['in_memory']:
    db_location = 'file:memdb1?mode=memory&cache=shared'
  else:
    db_location = os.path.join(cache_location, 'intertext.db')
  db = sqlite3.connect(db_location, uri=True, timeout=60)
  if initialize:
    db.execute('PRAGMA synchronous = EXTRA;') # OFF is fastest
    db.execute('PRAGMA journal_mode = DELETE;') # WAL is fastest
  if kwargs['in_memory']:
    db.execute('PRAGMA temp_store = 2;')
  else:
    db.execute('PRAGMA temp_store = 1;')
    db.execute('PRAGMA temp_store_directory = "{}"'.format(cache_location))
  return db


##
# DB Setters
##


def write_hashbands(writes, **kwargs):
  '''Given a db cursor and list of write operations, insert each'''
  if not writes: return []
  if kwargs.get('db') == 'sqlite':
    try:
      if kwargs['verbose']: print(' * writing', len(writes), 'hashbands')
      with closing(get_db(**kwargs)) as db:
        cursor = db.cursor()
        cursor.executemany('INSERT INTO hashbands (hashband, file_id, window_id) VALUES (?,?,?);', writes)
        db.commit()
    except sqlite3.DatabaseError:
      repair_database(**kwargs)
      return write_hashbands(writes, **kwargs)
  else:
    d = defaultdict(list)
    for hashband, file_id, window_id in writes:
      d[hashband].append([file_id, window_id])
    for hashband in d:
      out_dir = os.path.join('db', 'hashbands', hashband[0:2])
      make_dir(out_dir)
      path = os.path.join(out_dir, hashband[2:4])
      with open(path, 'a') as out:
        s = ''
        for r in d[hashband]:
          s += field_delimiter.join([str(v) for v in [hashband] + r]) + row_delimiter
        out.write(s)


def write_candidates(writes, **kwargs):
  '''Given a db cursor and list of write operations, insert each'''
  if not writes: return
  if kwargs.get('db') == 'sqlite':
    try:
      if kwargs['verbose']: print(' * writing', len(writes), 'candidates')
      with closing(get_db(**kwargs)) as db:
        cursor = db.cursor()
        cursor.executemany('INSERT OR IGNORE INTO candidates (file_id_a, file_id_b, window_id_a, window_id_b) VALUES (?,?,?,?);', writes)
        db.commit()
    except sqlite3.DatabaseError:
      repair_database(**kwargs)
      return write_candidates(writes, **kwargs)
  else:
    d = defaultdict(lambda: defaultdict(list))
    for row in writes:
      file_id_a, file_id_b, window_id_a, window_id_b = row
      d[file_id_a][file_id_b].append([window_id_a, window_id_b])
    for file_id_a in d:
      for file_id_b in d[file_id_a]:
        out_dir = os.path.join('db', 'candidates', str(file_id_a))
        make_dir(out_dir)
        path = os.path.join(out_dir, str(file_id_b))
        s = ''
        for row in d[file_id_a][file_id_b]:
          s += field_delimiter.join([str(v) for v in row]) + row_delimiter
        with open(path, 'a') as out:
          out.write(s)


def write_matches(writes, **kwargs):
  '''Given a db cursor and list of write operations, insert each'''
  if kwargs.get('db') == 'sqlite':
    try:
      if writes:
        if kwargs['verbose']: print(' * writing', len(writes), 'matches')
        with closing(get_db(**kwargs)) as db:
          cursor = db.cursor()
          cursor.executemany('INSERT INTO matches (file_id_a, file_id_b, window_id_a, window_id_b, similarity) VALUES (?,?,?,?,?);', writes)
          db.commit()
      return []
    except sqlite3.DatabaseError:
      repair_database(**kwargs)
      return write_matches(writes, **kwargs)
  else:
    d = defaultdict(lambda: defaultdict(list))
    for row in writes:
      file_id_a, file_id_b, window_id_a, window_id_b, sim = row
      d[file_id_a][file_id_b].append([window_id_a, window_id_b, sim])
    for file_id_a in d:
      for file_id_b in d[file_id_a]:
        out_dir = os.path.join('db', 'matches', str(file_id_a))
        make_dir(out_dir)
        path = os.path.join(out_dir, str(file_id_b))
        s = ''
        for row in d[file_id_a][file_id_b]:
          s += field_delimiter.join([str(v) for v in row]) + row_delimiter
        with open(path, 'a') as out:
          out.write(s)


def repair_database(**kwargs):
  '''Attempt to repair the db in a process-safe manner'''
  raise sqlite3.DatabaseError


##
# DB Getters
##


def stream_hashbands(**kwargs):
  '''Stream [hashband, file_id, window_id] sorted by hashband'''
  if kwargs.get('db') == 'sqlite':
    with closing(get_db(**kwargs)) as db:
      cursor = db.cursor()
      rows = []
      for row in cursor.execute('''
        WITH file_id_counts AS (
          SELECT hashband, COUNT(DISTINCT(file_id)) as count
          FROM hashbands
          GROUP BY hashband
          HAVING COUNT > 1
        ) SELECT hashband, file_id, window_id
          FROM hashbands
          WHERE hashband IN (SELECT hashband from file_id_counts)
          ORDER BY hashband
      '''):
        yield row
  else:
    for i in glob.glob(os.path.join('db', 'hashbands', '*', '*')):
      d = defaultdict(list)
      with open(i) as f:
        f = f.read()
      for row in f.split(row_delimiter):
        if not row: continue
        hashband, file_id, window_id = row.split(field_delimiter)
        d[hashband].append([int(file_id), int(window_id)])
      for hashband in d:
        file_ids, window_ids = zip(*d[hashband])
        if len(set(file_ids)) > 1:
          for j in d[hashband]:
            yield [hashband] + j


def stream_candidate_file_id_pairs(**kwargs):
  '''Stream [file_id_a, file_id_b] pairs for files with matching hashbands'''
  if kwargs.get('db') == 'sqlite':
    with closing(get_db(**kwargs)) as db:
      cursor = db.cursor()
      for row in cursor.execute('''
        SELECT DISTINCT file_id_a, file_id_b
        FROM candidates
        ORDER BY file_id_a, file_id_b
      '''):
        yield row
  else:
    for i in glob.glob(os.path.join('db', 'candidates', '*')):
      file_id_a = os.path.split(i)[-1]
      for j in glob.glob(os.path.join(i, '*')):
        file_id_b = os.path.split(j)[-1]
        yield [int(file_id_a), int(file_id_b)]


def stream_matching_candidate_windows(file_id_a, file_id_b, **kwargs):
  '''Stream [file_id_a, file_id_b, window_id_a, window_id_b] for matching hashbands'''
  if kwargs.get('db') == 'sqlite':
    with closing(get_db(**kwargs)) as db:
      cursor = db.cursor()
      for i in cursor.execute('''
          SELECT DISTINCT file_id_a, file_id_b, window_id_a, window_id_b
          FROM candidates
          WHERE file_id_a = ? AND file_id_b = ?
          ORDER BY file_id_b
        ''', (file_id_a, file_id_b,)):
        yield i
  else:
    with open(os.path.join('db', 'candidates', str(file_id_a), str(file_id_b))) as f:
      f = f.read()
    for row in f.split(row_delimiter):
      if not row: continue
      yield [int(file_id_a), int(file_id_b)] + [int(i) for i in row.split(field_delimiter)]


def stream_matching_file_id_pairs(**kwargs):
  '''Stream [file_id_a, file_id_b] for file ids that have verified matches'''
  if kwargs.get('db') == 'sqlite':
    with closing(get_db(**kwargs)) as db:
      cursor = db.cursor()
      for i in cursor.execute('SELECT DISTINCT file_id_a, file_id_b FROM matches;'):
        yield i
  else:
    for i in glob.glob(os.path.join('db', 'matches', '*')):
      file_id_a = os.path.split(i)[-1]
      for j in glob.glob(os.path.join(i, '*')):
        file_id_b = os.path.split(j)[-1]
        yield [int(file_id_a), int(file_id_b)]


def stream_file_pair_matches(file_id_a, file_id_b, **kwargs):
  '''Stream [file_id_a, file_id_b, window_id_a, window_id_b, similarity] for a match pair'''
  if kwargs.get('db') == 'sqlite':
    with closing(get_db(**kwargs)) as db:
      cursor = db.cursor()
      for i in cursor.execute('SELECT * FROM matches WHERE file_id_a = ? AND file_id_b = ?', (file_id_a, file_id_b,)):
        yield i
  else:
    with open(os.path.join('db', 'matches', str(file_id_a), str(file_id_b))) as f:
      f = f.read()
      for row in f.split(row_delimiter):
        if not row: continue
        yield [int(file_id_a), int(file_id_b)] + [int(j) for j in row.split(field_delimiter)]


def stream_match_lists(**kwargs):
  '''Stream a stream of (file_id, [match, match, ...]) objects'''
  for i in glob.glob(os.path.join(kwargs['output'], 'api', 'matches', '*')):
    file_id = os.path.basename(i).replace('.json', '')
    with open(i) as f:
      match_list = json.load(f)
      yield (file_id, match_list)


##
# Banishing matches
##


def noop():
  # banish matches
  if kwargs['banish_glob']:
    print(' * banishing matches')
    g = networkx.Graph()
    for file_id_a in matches_d:
      for file_id_b in matches_d[file_id_a]:
        for m in matches_d[file_id_a][file_id_b]:
          g.add_edge(f'{file_id_a}.{m[0]}', f'{file_id_b}.{m[1]}')
    # map file_id.segment_id segments to whether or not they're banished
    banished_set = set()
    distances = dict(networkx.all_pairs_shortest_path_length(g))
    for i in list(connected_components(g)):
      banished_ids = [j for j in i if int(j.split('.')[0]) in banished_file_ids]
      # search up to maximum path length between nodes so nodes linked to a banished node are removed
      for j in i:
        if any([distances[j][k] < kwargs['banish_distance'] for k in banished_ids]):
          banished_set.add(j)
    # apply the banish filter
    for file_id_a in list(matches_d):
      for file_id_b in list(matches_d[file_id_a]):
        l = []
        for window_a, window_b, sim in matches_d[file_id_a][file_id_b]:
          if (f'{file_id_a}.{window_a}' not in banished_set) and \
             (f'{file_id_b}.{window_b}' not in banished_set):
            l.append([window_a, window_b, sim])
        matches_d[file_id_a][file_id_b] = l


def to_graph(l):
  '''Given a 2D array, return a networkx.Graph object'''
  G = networkx.Graph()
  # i is a list of nodes that share edges
  for i in l:
    G.add_nodes_from(i)
    G.add_edges_from(to_edges(i))
  return G


def to_edges(l):
  '''Given a list of elements that share edges in a graph, iterate those edges pairwise'''
  iterator = iter(l)
  last = next(iterator)
  for current in iterator:
    yield last, current
    last = current


##
# Shared
##

@functools.lru_cache(maxsize=1024)
def get_words(path, **kwargs):
  '''Given a file path return a list of strings from that file'''
  with codecs.open(path, 'r', kwargs['encoding']) as f:
    if kwargs['xml_base_tag']:
      soup = BeautifulSoup(f, 'html.parser').find(kwargs['xml_base_tag'].lower())
      if kwargs['xml_remove_tags']:
        [soup.extract(i) for i in kwargs['xml_remove_tags']]
      f = soup.get_text()
    else:
      f = f.read()
    if kwargs['strip_diacritics'] and not kwargs.get('display', False):
      f = unidecode(f)
  # format the list of words
  if kwargs.get('display', False):
    NEWLINE = '__NEWLINE__'
    l = f.replace('\n', ' ' + NEWLINE + ' ').split()
    formatted = []
    for idx, i in enumerate(l):
      if i == NEWLINE:
        if formatted and formatted[-1] += '<br/>'
      else:
        formatted.append(i)
    return formatted
  else:
    return f.split()


@functools.lru_cache(maxsize=1024)
def get_windows(path, **kwargs):
  '''Given a file path return a list of strings from that file'''
  words = get_words(path, **kwargs)
  l = []
  for idx, window in enumerate(list(ngrams(words, kwargs['window_length']))):
    if idx % kwargs['slide_length'] != 0:
      continue
    l.append(' '.join(window))
  return l


def get_cacheable(*args):
  '''Given a dictionary of kwargs return a dictionary with cacheable values retained'''
  kwargs = args[0]
  if len(args) > 1:
    for i in args[1:]:
      kwargs.update(i)
  return {k: kwargs[k] for k in kwargs if isinstance(kwargs[k], Hashable)}


def make_dir(path):
  '''Make a directory if it doesn't exist'''
  if not os.path.exists(path):
    try:
      os.makedirs(path)
    except:
      pass


if __name__ == '__main__':
  parse()
