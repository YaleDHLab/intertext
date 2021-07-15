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
import random
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
  'exclude_glob': '',
  'output': 'output',
  'metadata': {},
  'encoding': 'utf8',
  'xml_base_tag': None,
  'xml_remove_tags': tuple(),
  'xml_page_tag': None,
  'xml_page_attr': None,
  'batch_size': 10**5,
  'write_frequency': 10**5,
  'chargram_length': 4,
  'window_length': 14,
  'slide_length': 4,
  'hashband_length': 4,
  'hashband_step': 3,
  'banish_distance': 4,
  'min_sim': 50,
  'excluded_file_ids': tuple(),
  'banish_file_ids': tuple(),
  'max_file_sim': None,
  'client': '0.0.1a',
  'update_client': False,
  'strip_diacritics': False,
  'db': 'sqlite',
  'only': None,
  'update_metadata': False,
  'verbose': False,
}


'''
TODO:
  * add flag to indicate if same-author matches are allowed
  * add support for CSV metadata
  * add support for xml + txt in same run
  * add MySQL db backend
  * if resuming, process output/files.json to get the files and file ids
'''


# path globals
source_location = os.path.dirname(os.path.realpath(__file__))
client_location = os.path.join(source_location, 'client')
cache_location = os.path.join(os.getcwd(), 'cache')


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
  parser.add_argument('--banish', '-b', type=str, default=config['banish_glob'], dest='banish_glob', help='path to a glob of text files to banish from matches', required=False)
  parser.add_argument('--exclude', type=str, default=config['exclude_glob'], dest='exclude_glob', help='path to a glob of text files to exclude from matches', required=False)
  parser.add_argument('--metadata', '-m', type=str, default=config['metadata'], help='path to a JSON metadata file (see README)', required=False)
  parser.add_argument('--encoding', '-e', type=str, default=config['encoding'], help='the encoding of infiles', required=False)
  parser.add_argument('--window_length', '-w', type=int, default=config['window_length'], help='the length of windows when processing files (see README)', required=False)
  parser.add_argument('--hashband_length', '-hb', type=int, default=config['hashband_length'], help='the number of minhash values per hashband', required=False)
  parser.add_argument('--hashband_step', '-hs', type=int, default=config['hashband_step'], help='the number of minhash units to slide hashband windows', required=False)
  parser.add_argument('--chargram_length', '-cl', type=int, default=config['chargram_length'], help='the number of characters per character shingle', required=False)
  parser.add_argument('--write_frequency', '-wf', type=int, default=config['write_frequency'], help='the max number of write operations to store in RAM')
  parser.add_argument('--slide_length', '-l', type=int, default=config['slide_length'], help='the length to slide windows when processing files (see README)', required=False)
  parser.add_argument('--banish_distance', '-bd', type=int, default=config['banish_distance'], help='the graph distance to travel when banishing linked matches', required=False)
  parser.add_argument('--min_sim', '-s', type=int, default=config['min_sim'], help='the minimum similarity of matches to retain)', required=False)
  parser.add_argument('--max_file_sim', '-fs', type=int, default=config['max_file_sim'], help='the maximum similarity between two files such that matches are retained', required=False)
  parser.add_argument('--output', '-o', type=str, default=config['output'], help='the output location', required=False)
  parser.add_argument('--client', '-c', type=str, default=config['client'], help='the client version to fetch and display', required=False)
  parser.add_argument('--xml_base_tag', type=str, default=config['xml_base_tag'], help='if specified, text within this parent tag will be parsed', required=False)
  parser.add_argument('--xml_remove_tags', type=tuple, default=config['xml_remove_tags'], help='if specified, text within these tags will be removed', required=False)
  parser.add_argument('--xml_page_tag', type=str, default=config['xml_page_tag'], help='if specified, urls can reference content within this tag')
  parser.add_argument('--xml_page_attr', type=str, default=config['xml_page_attr'], help='if specified, urls can reference content within this attr of xml_page_tag')
  parser.add_argument('--strip_diacritics', default=config['strip_diacritics'], help='if specified, diacritics will be parsed from texts during processing', required=False, action='store_true')
  parser.add_argument('--update_client', default=config['update_client'], help='boolean indicating whether to update the stored client', required=False, action='store_true')
  parser.add_argument('--verbose', '-v', default=config['verbose'], help='if specified, the intertext process will log more operations', required=False, action='store_true')
  parser.add_argument('--db', default=config['db'], help='specify sqlite to use a sqlite db', required=False)
  parser.add_argument('--only', default=config['only'], help='only retain matches that include text from the specified file path', required=False)
  parser.add_argument('--update_metadata', default=config['update_metadata'], help='skip all processing and only update the metadata for a plot', action='store_true')
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

  # identify the infiles
  kwargs = process_kwargs(**kwargs)

  # create the output directories where results will be stored
  prepare_output_directories(**kwargs)

  # update the metadata and exit if requested
  if not kwargs.get('update_metadata'):

    # remove extant db and prepare output directories
    clear_db(**kwargs)

    # save JSON with the list of infiles
    with open(os.path.join(kwargs['output'], 'api', 'files.json'), 'w') as out:
      json.dump(kwargs['infiles'], out)  # save JSON with the list of infiles

    # create the db
    initialize_db('hashbands', **kwargs)
    initialize_db('candidates', **kwargs)
    initialize_db('matches', **kwargs)

    # minhash files & store hashbands in db
    print(' * creating minhashes - using CUDA:', CUDA_AVAILABLE)
    get_all_hashbands(**kwargs)

    # find all hashbands that have multiple distict file_ids
    print(' * identifying match candidates')
    get_all_match_candidates(**kwargs)

    # validate matches from among the candidates
    print(' * validating matches')
    validate_all_matches(**kwargs)

  # banish matches if necessary
  if kwargs['banish_glob']: banish_matches(**kwargs)

  # format matches into JSON for client consumption
  print(' * formatting matches')
  format_all_matches(**kwargs)

  # combine all matches into a single match object
  print(' * formatting JSON outputs')
  create_all_match_json(**kwargs)


def process_kwargs(**kwargs):
  '''Return a list of the infiles to be processed'''

  # check xml page kwargs
  if kwargs.get('xml_page_tag') and not kwargs.get('metadata'):
    raise Exception('--xml_page_tag requires --metadata to be provided')

  # typecheck inputs
  assert kwargs['min_sim'] >= 1 and kwargs['min_sim'] <= 100

  # get the list of infiles
  infiles = sorted(glob.glob(kwargs['infile_glob']))
  if len(infiles) == 0:
    raise Exception('No infiles could be found!')

  # identify banished files and add to infiles
  if kwargs['banish_glob']:
    banished_files = sorted(glob.glob(kwargs['banish_glob']))
    infiles += banished_files
    banished_file_set = set(banished_files)
    banished_file_ids = set()
    for file_idx, file in enumerate(infiles):
      if file in banished_file_set:
        banished_file_ids.add(file_idx)
    kwargs['banished_file_ids'] = tuple(banished_file_ids)
  kwargs['infiles'] = infiles

  # identify excluded files and their file ids
  if kwargs['exclude_glob']:
    exclude_set = set(sorted(glob.glob(kwargs['exclude_glob'])))
    excluded_file_ids = set()
    for file_idx, file in enumerate(infiles):
      if file in exclude_set:
        excluded_file_ids.add(file_idx)
    kwargs['excluded_file_ids'] = tuple(excluded_file_ids)

  # get the metadata (if any)
  kwargs['metadata'] = get_metadata(**kwargs)

  # get the focal text index (if any)
  kwargs['only_index'] = get_only_index(**kwargs)

  # return the processed kwargs
  return kwargs


def get_metadata(**kwargs):
  '''if the user provided metadata, store it in the kwargs'''
  metadata = json.load(open(kwargs['metadata'])) if kwargs['metadata'] else {}
  for i in kwargs['infiles']:
    basename = os.path.basename(i)
    if basename not in metadata:
      metadata[basename] = {
        'author': 'Unknown',
        'title': os.path.basename(i),
      }
  return metadata


def prepare_output_directories(**kwargs):
  '''Create the folders that store output objects'''
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


def get_only_index(**kwargs):
  '''Return the index number of the only file from which matches should be retained'''
  if kwargs.get('only', None) != None:
    return kwargs['infiles'].index(kwargs['only'])
  else:
    return None


def clear_db(**kwargs):
  '''Clear the extant db'''
  if os.path.isdir('db'):
    shutil.rmtree('db')
  for i in glob.glob(os.path.join('cache', '*.db')):
    os.remove(i)


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
  hashbands = set()
  for window_idx, minhash in enumerate(minhashes):
    for hdx, h in enumerate(ngrams(minhash, kwargs['hashband_length'])):
      if hdx % kwargs['hashband_step'] == 0:
        hashbands.add(tuple(['.'.join([str(i) for i in h]), file_idx, window_idx]))
  write_hashbands(hashbands, **kwargs)


def get_file_minhashes(file_path, **kwargs):
  '''Return the minhash array for a file'''
  minhash_path = os.path.join(cache_location, 'minhashes', file_path.replace(os.path.sep, '___') + '.npy')
  if os.path.exists(minhash_path):
    print(' * loading', file_path, 'minhashes from cache')
    return np.load(minhash_path)
  # run minhash algorithm on file
  l = []
  for window_idx, window in enumerate(get_windows(file_path, **get_cacheable(kwargs))):
    char_hashes = fastNGramHashes(window.lower().encode(kwargs['encoding']), n=kwargs['chargram_length'])
    fingerprint = hasher.fingerprint(char_hashes, cuda=CUDA_AVAILABLE)
    l.append(fingerprint)
  minhashes = np.array(l)
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
  if kwargs['verbose']: print(' * processing match candidate block')
  pool = multiprocessing.Pool()
  l = list(subdivide(l, len(l) // multiprocessing.cpu_count()))
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
      last_hashband = hashband
      if kwargs.get('only_index') != None:
        if not any([i[0] == kwargs['only_index'] for i in hashband_values]):
          continue
      for a, b in combinations(hashband_values, 2):
        if kwargs.get('only_index') != None:
          if a[0] != kwargs['only_index'] and b[0] != kwargs['only_index']:
            continue
        # skip same file matches
        if a[0] == b[0]:
          continue
        elif a[0] < b[0]:
          results.append(tuple([a[0], b[0], a[1], b[1]]))
        else:
          results.append(tuple([b[0], a[0], b[1], a[1]]))
      hashband_values = set([tup])
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
    try:
      text_a = file_a_windows[window_id_a]
      text_b = file_b_windows[window_id_b]
    except:
      print(' * window lookup OOB')
      print(file_id_a, window_id_a, len(file_a_windows), kwargs['infiles'][file_id_a])
      print(file_id_b, window_id_b, len(file_b_windows), kwargs['infiles'][file_id_b])
      continue
    # run a fast pass to measure
    sim = ratio(text_a, text_b) * 100
    if sim > (kwargs['min_sim'] * 0.85):
      sim = SequenceMatcher(None, text_a, text_b, autojunk=False).ratio() * 100
      if sim >= kwargs['min_sim']:
        # remove matches with predominance of single character words
        a_singles = [i for i in text_a.split() if len(i) == 1]
        b_singles = [i for i in text_b.split() if len(i) == 1]
        if len(a_singles) >= (kwargs['window_length'] * 0.75) or \
           len(b_singles) >= (kwargs['window_length'] * 0.75):
          continue
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
  if kwargs.get('excluded_file_ids'):
    if file_id_a in kwargs['excluded_file_ids'] or file_id_b in kwargs['excluded_file_ids']:
      return
  l = list(stream_file_pair_matches(file_id_a, file_id_b, **kwargs))
  if not l: return
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
  path_a = kwargs['infiles'][file_id_a]
  path_b = kwargs['infiles'][file_id_b]
  bn_a = os.path.basename(path_a)
  bn_b = os.path.basename(path_b)
  a_meta = kwargs.get('metadata', {}).get(bn_a, {})
  b_meta = kwargs.get('metadata', {}).get(bn_b, {})
  # format the matches
  a_words = get_words(path_a, **get_cacheable(kwargs, {'display': True}))
  b_words = get_words(path_b, **get_cacheable(kwargs, {'display': True}))
  formatted = []
  # fetch a mapping from window id to $PAGE elements if necessary
  a_windows_to_page = None
  b_windows_to_page = None
  try:
    a_windows_to_page = get_window_map(path_a, **get_cacheable(kwargs))
    b_windows_to_page = get_window_map(path_b, **get_cacheable(kwargs))
  except:
    print(' * unable to retrieve mapping from window to page id')
  # each member c in clusters is a dictionary {a: b: } where values contain the match windows
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
      'source_filename': bn_a,
      'target_filename': bn_b,
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
      'source_url': get_url(a_meta, a_windows_to_page, c['a'], **kwargs),
      'target_url': get_url(b_meta, b_windows_to_page, c['b'], **kwargs),
    })
  return(formatted)


def get_url(meta, windows_to_page, windows, **kwargs):
  '''Return the url to the first of the current windows'''
  if not kwargs.get('xml_page_tag'): return meta.get('url', '')
  return meta.get('url').replace('$PAGE_ID', windows_to_page.get(windows[0], ''))


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
    'prematch': ' '.join(words[max(0, start-kwargs['window_length']):start]).lstrip('<br/>'),
    'match': ' '.join(words[start:end]),
    'postmatch': ' '.join(words[end:end + kwargs['window_length']]).rstrip('<br/>'),
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
  authors = []
  titles = []
  for i in kwargs['infiles']:
    if kwargs.get('excluded_file_ids'):
      if i in kwargs['excluded_file_ids']:
        continue
    if kwargs.get('banished_file_ids'):
      if i in kwargs['banished_file_ids']:
        continue
    authors.append(kwargs['metadata'].get(os.path.basename(i), {}).get('author', 'Unknown'))
    titles.append(kwargs['metadata'].get(os.path.basename(i), {}).get('title', os.path.basename(i)))
  author_d = defaultdict(list)
  title_d = defaultdict(list)
  for idx, i in enumerate(authors): author_d[i].append(idx)
  for idx, i in enumerate(titles): title_d[i].append(idx)
  with open(os.path.join(kwargs['output'], 'api', 'authors.json'), 'w') as out:
    json.dump(author_d, out)
  with open(os.path.join(kwargs['output'], 'api', 'titles.json'), 'w')  as out:
    json.dump(title_d, out)

  # create minimal representations of all matches to be sorted by each sort heuristic below
  l = set()
  for file_id, matches in stream_match_lists(**kwargs):
    for match_idx, match in enumerate(matches):
      if int(file_id) != int(match.get('source_file_id')): continue
      l.add(tuple([
        match_idx,
        match.get('source_file_id'),
        match.get('target_file_id'),
        match.get('similarity', ''),
        match.get('source_author' ''),
        match.get('source_title', ''),
        match.get('source_year', ''),
      ]))

  # create and store the file_id.match_index indices for each sort heuristic
  l = list(l)
  for label, idx in [['similarity', -4], ['author', -3], ['title', -2], ['year', -1]]:
    sorted_list = sorted(l, key=lambda j: j[idx])
    ids = [[int(k) if is_number(k) else k for k in i[:4]] for i in sorted_list]
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


def initialize_db(db_name, **kwargs):
  '''Run all setup steps to create the database'''
  if kwargs.get('db') == 'sqlite':
    with closing(get_db(db_name, initialize=True, **kwargs)) as db:
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


def get_db(db_name, initialize=False, **kwargs):
  '''Return a Sqlite DB'''
  db_location = os.path.join(cache_location, '{}.db'.format(db_name))
  db = sqlite3.connect(db_location, uri=True, timeout=2**16)
  if initialize:
    db.execute('PRAGMA synchronous = EXTRA;') # OFF is fastest
    db.execute('PRAGMA journal_mode = DELETE;') # WAL is fastest
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
      with closing(get_db('hashbands', **kwargs)) as db:
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
      with closing(get_db('candidates', **kwargs)) as db:
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
        with closing(get_db('matches', **kwargs)) as db:
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


def delete_matches(banished_dict, **kwargs):
  '''Given d[file_id] = [window_id], delete all specified windows'''
  if kwargs.get('db') == 'sqlite':
    deletes = []
    for file_id, window_ids in banished_dict.items():
      deletes += [(file_id, i, file_id, i) for i in window_ids]
    if kwargs['verbose']: print(' * deleting', len(deletes), 'matches')
    with closing(get_db('matches', **kwargs)) as db:
      cursor = db.cursor()
      cursor.executemany('DELETE FROM matches WHERE file_id_a = (?) AND window_id_a = (?) OR file_id_b = (?) and window_id_b = (?) ', deletes)
      db.commit()
  else:
    for file_id in banished_dict:
      files = glob.glob(os.path.join('db', 'matches', file_id, '*'))
      for i in files:
        with open(i) as f:
          lines = []
          for l in f.read().strip().split(row_delimiter):
            window_id_a, window_id_b, sim = l.split(field_delimiter)
            if window_id_a not in banished_dict[file_id]:
              lines.append(l)
        # write the cleaned lines to disk
        with open(i, 'w') as out:
          out.write(row_delimiter.join(lines))


def repair_database(**kwargs):
  '''Attempt to repair the db in a process-safe manner'''
  raise sqlite3.DatabaseError


##
# DB Getters
##


def stream_hashbands(**kwargs):
  '''Stream [hashband, file_id, window_id] sorted by hashband'''
  if kwargs.get('verbose'): print(' * querying for hashbands')
  if kwargs.get('db') == 'sqlite':
    with closing(get_db('hashbands', **kwargs)) as db:
      cursor = db.cursor()
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
      # accumulate file_id, window id values by hashband to effectively sort by hashband
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
  if kwargs.get('verbose'): print(' * querying for candidate file id pairs')
  if kwargs.get('db') == 'sqlite':
    with closing(get_db('candidates', **kwargs)) as db:
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
  if kwargs.get('verbose'): print(' * querying for matching candidate windows')
  if kwargs.get('db') == 'sqlite':
    with closing(get_db('candidates', **kwargs)) as db:
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
    with closing(get_db('matches', **kwargs)) as db:
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
    with closing(get_db('matches', **kwargs)) as db:
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


def banish_matches(**kwargs):
  '''Delete banished matches from the db'''
  if not kwargs['banish_glob']: return
  print(' * banishing matches')
  g = networkx.Graph()
  for file_id_a, file_id_b in stream_matching_file_id_pairs(**kwargs):
    l = stream_file_pair_matches(file_id_a, file_id_b, **kwargs)
    for _, _, window_a, window_b, sim in l:
      s = '{}.{}'.format(file_id_a, window_a)
      t = '{}.{}'.format(file_id_b, window_b)
      g.add_edge(s, t)
  # create d[file_id] = [window_id, window_id] of banished windows
  banished_dict = defaultdict(set)
  distances = dict(networkx.all_pairs_shortest_path_length(g))
  for i in list(connected_components(g)):
    banished_ids = [j for j in i if int(j.split('.')[0]) in kwargs['banished_file_ids']]
    # search up to maximum path length between nodes so nodes linked to a banished node are removed
    for j in i:
      if any([distances[j][k] < kwargs['banish_distance'] for k in banished_ids]):
        file_id, window_id = j.split('.')
        banished_dict[file_id].add(window_id)
  # remove the banished file_id, window_id tuples from the db
  delete_matches(banished_dict, **kwargs)


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
  with get_file_handler(path, **kwargs) as f:
    if kwargs['xml_base_tag']:
      soup = get_soup(f, **kwargs)
      f = soup.get_text()
    else:
      f = f.read()
  # optionally remove diacritics
  if kwargs['strip_diacritics'] and not kwargs.get('display', False):
    f = unidecode(f)
  # optionally format the list of words for display in the web viewer
  if kwargs.get('display', False):
    NEWLINE = '__NEWLINE__'
    l = f.replace('\n', ' ' + NEWLINE + ' ').split()
    formatted = []
    for idx, i in enumerate(l):
      if i == NEWLINE:
        if formatted: formatted[-1] += '<br/>'
      else:
        formatted.append(i)
    return formatted
  else:
    return f.split()


def get_file_handler(path, **kwargs):
  '''Given the path to a file return a _io.TextIOWrapper object in 'r' mode'''
  return codecs.open(path, 'r', kwargs['encoding'])


def get_soup(f, **kwargs):
  '''Return a soup object given a _io.TextIOWrapper object'''
  soup = BeautifulSoup(f, 'html.parser').find(kwargs['xml_base_tag'].lower())
  # remove any specified xml tags
  if kwargs['xml_remove_tags']: [soup.extract(i) for i in kwargs['xml_remove_tags']]
  return soup


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


@functools.lru_cache(maxsize=1024)
def get_window_map(path, **kwargs):
  '''Get a mapping from window id to window metadata, including page id'''
  xml_page_tag = kwargs.get('xml_page_tag')
  xml_page_attr = kwargs.get('xml_page_attr')
  if not xml_page_tag: return
  xml_page_tag = xml_page_tag.lower()
  xml_page_attr = xml_page_attr.lower() if xml_page_attr else None
  # read the text document
  with get_file_handler(path, **kwargs) as f:
    f = f.read().lower()
  # split on page breaks using string operations
  pagebreak = '{}_$PB$_{}'.format(random.randint(0, 2**32), random.randint(0, 2**32)).lower()
  f = f.replace('<{} '.format(xml_page_tag), pagebreak)
  f = f.replace('<{}/>'.format(xml_page_tag), pagebreak)
  pages = f.split(pagebreak)
  # populate the mapping from window index to page id d[window_index] = {page_id, ...}
  d = {}
  window_id = 0
  # skip content leading up to first page
  for page_index, page in enumerate(pages[1:]):
    # handle case of page id specified in an attribute
    if xml_page_attr:
      tag = page.split('>')[0]
      page_id = tag.split('{}='.format(xml_page_attr))[1].split(' ')[0]
      page_id = page_id.replace('"', '').replace("'", '')
      page_id = page_id.rstrip('/>')
    # hande case of page id between tags
    elif '</' + xml_page_tag in page:
      page_id = page.split('</' + xml_page_tag)[0]
      if '>' in page_id: page_id = page_id.split('>')[1]
    # handle case of sequential pages without identification (self-closing tags)
    else:
      page_id = page_index
    # clean the page id
    page_id = str(page_id).strip()
    # remove the lead tag
    page = '>'.join(page.split('>')[1:])
    soup = BeautifulSoup(page, 'html.parser')
    text = soup.get_text()
    words = text.split()
    for word_index, word in enumerate(words):
      if word_index and (word_index % kwargs['slide_length'] == 0):
        window_id += 1
      d[window_id] = page_id
  return d


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


def is_number(s):
  '''Return a bool indicating whether s is a number'''
  try:
    float(s)
    return True
  except:
    return False


if __name__ == '__main__':
  parse()
