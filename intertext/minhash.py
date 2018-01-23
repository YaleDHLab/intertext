from __future__ import division
from multiprocessing import Pool
from collections import defaultdict
from datasketch import MinHash
from difflib import SequenceMatcher
from pymongo import MongoClient
from shutil import rmtree
from nltk import ngrams
from bs4 import BeautifulSoup
import glob, json, sys, os

##
# Filter keys - identify hashbands that occur 2+ times
##

def count_hashbands():
  print(' * counting hashbands')
  rm_dirs('tmp')
  pool = Pool(config['max_cores'])
  for c, _ in enumerate(pool.imap(count_file_hashbands, text_ids)):
    print(' * counted', c+1, 'of', len(infiles), 'hashbands')
  pool.close()
  pool.join()

def count_file_hashbands(text_id):
  text_path = infiles[int(text_id)]
  for window_id, window in enumerate(get_windows(text_path)):
    for hashband in get_hashbands(window):
      a, b = hashband[0:2], hashband[2:4]
      outdir = os.path.join('tmp', 'hashbands', a, b)
      make_dirs(outdir)
      with open( os.path.join(outdir, a + b), 'a') as out:
        out.write(hashband + '-' + text_id + '.' + str(window_id) + '#')

def rm_dirs(path):
  try:
    rmtree('tmp')
  except FileNotFoundError:
    pass

def make_dirs(path):
  try:
    os.makedirs(path)
  except FileExistsError:
    pass

def get_windows(text_path):
  with open(text_path) as f:
    words = get_text_content(f.read()).lower().split()
  for window_id, window in enumerate(ngrams(words, config['window_size'])):
    if window_id % config['step'] == 0:
      yield window

def get_hashbands(window):
  minhash = MinHash(num_perm=config['n_permutations'], seed=1)
  for ngram in set(ngrams(' '.join(window), 3)):
    minhash.update( ''.join(ngram).encode('utf8') )
  hashband_vals = []
  for c, i in enumerate(minhash.hashvalues):
    hashband_vals.append(i)
    if len(hashband_vals) == config['hashband_length']:
      hashband = '.'.join([str(j) for j in hashband_vals])
      hashband_vals = []
      yield hashband

def get_text_content(s):
  if config['xml_tag']:
    parser = 'html.parser' if infiles[0].split('.')[-1] == '.html' else 'lxml'
    soup = BeautifulSoup(s, parser).find(config['xml_tag'])
    return soup.get_text()
  return s

##
# Use hashband datastore to map file_ids to list of matching_file_ids
##

def match_minhash_keys():
  dirs = glob.glob(os.path.join('tmp', 'hashbands', '*',))
  pool = Pool(config['max_cores'])
  for c, _ in enumerate(pool.imap(match_minhash_key, dirs)):
    c += 1
    if c % 100 == 0:
      print(' * processed', c, 'of', len(dirs), 'minhash blocks')
  pool.close()
  pool.join()

def match_minhash_key(_dir):
  '''
  dir contains a number of subdirectories, each of which contains
  a file with hashbands and segment identifiers. For each hashband
  in any file that occurs in multiple distinct files, find all
  combinations among the file_id and segment_ids in which it occurs
  and store each with the following structure: d[fid] =
  [fid.sid, fid'.sid'], where fid is a file id, sid is a segment id,
  and given two file ids, the higher of the two is designated fid while
  the lower is designated fid'. Then save the values of each fid key
  to a file in tmp/matches/{{ fid }}
  '''
  d = defaultdict(list)
  count = 0
  for _file in glob.glob(os.path.join(_dir, '*', '*')):
    for matching_ids in get_matching_ids(_file):
      for id_pair in ngrams(matching_ids, 2):
        file_id_a, file_segment_a = id_pair[0].split('.')
        file_id_b, file_segment_b = id_pair[1].split('.')
        if file_id_a != file_id_b:
          if file_id_a > file_id_b:
            d[file_id_a].append( id_pair[0] + '-' + id_pair[1] )
          else:
            d[file_id_b].append( id_pair[1] + '-' + id_pair[0] )
          count += 1
        if count >= 10000:
          save_matches(d)
          d = defaultdict(list)
          count = 0
  if count:
    save_matches(d)

def get_matching_ids(file_path):
  '''
  file_path points to a file on disk with data in the following format:
  hashband-file_id.segment_id#. Partition the file contents into separate
  hashbands, store each in a dictionary, and return a list of lists
  where sublists have the format: [file_id.segment_id]
  '''
  matching_ids = []
  with open(file_path) as f:
    f = f.read()
  d = defaultdict(list)
  for i in f.split('#')[:-1]:
    hashband, file_ids = i.split('-')
    d[hashband].append(file_ids)
  for key in d:
    if len(d[key]) > 1:
      matching_ids.append(d[key])
  return matching_ids

def save_matches(d):
  '''
  d is a defaultdict whose keys represent file ids and whose values
  are lists of strings in the form: fid.sid-fid'.sid'. Add all
  file and segment ids for each file id to disk
  '''
  for i in d:
    outdir = os.path.join('tmp', 'matches')
    make_dirs(outdir)
    with open(os.path.join('tmp', 'matches', i), 'a') as out:
      out.write('#'.join(d[i]) + '#')

##
# Validate all proposed matches
##

def validate_all_matches():
  pool = Pool(config['max_cores'])
  match_files = glob.glob(os.path.join('tmp', 'matches', '*'))
  for c, _ in enumerate(pool.imap(validate_text_matches, match_files)):
    print(' * validated', c+1, 'of', len(match_files), 'file matches')
  pool.close()
  pool.join()

def validate_text_matches(match_file):
  '''
  match_file is a path to a file for which the filename is the file id
  and for which the file content is a sequence of items in the following
  format: fid.sid-fid'.sid'#. Partition out each of those matches,
  organize them into a dictionary with matching text ids as keys
  and match segment ids as values. Then for each matching text, compare
  the match segment pairs from `text_id` and the matching text to assess
  similarity. If the similarity is greater than `config.min_similarity`,
  add segment_id.match_segment_id to the set of values assigned to the
  true-matches-file_id-match_file_id key.
  '''
  validated = ''
  text_id = os.path.basename(match_file)
  text_matches = get_text_matches(match_file)
  text_grams = list( get_windows(infiles[int(text_id)]) )
  for match_text_id in text_matches:
    match_grams = list( get_windows(infiles[int(match_text_id)]) )
    for text_window_id, match_window_id in text_matches[match_text_id]:
      text_window = ' '.join(text_grams[ text_window_id ])
      match_window = ' '.join(match_grams[ match_window_id ])
      similarity = sim(text_window, match_window)
      if similarity >= config['min_similarity']:
        file_ids = text_id + '.' + str(text_window_id)
        match_ids = match_text_id + '.' + str(match_window_id)
        validated += file_ids + '-' + match_ids + '|' + str(similarity) + '#'
  save_validated_matches(text_id, validated)

def sim(a, b):
  return SequenceMatcher(None, a, b, autojunk=False).ratio()

def get_text_matches(match_file):
  '''
  match_file is the path to a file on disk with data in the format
  fid.sid-fid'.sid'. Parse out all matching segments and organize
  them into a dictionary with the format d[match_text_id] =
  [window_id, match_window_id]
  '''
  d = defaultdict(list)
  with open(match_file) as f:
    for i in f.read().split('#')[:-1]:
      file_ids, match_ids = i.split('-')
      file_id, file_segment_id = file_ids.split('.')
      match_id, match_segment_id = match_ids.split('.')
      d[match_id].append(( int(file_segment_id), int(match_segment_id) ))
  return d

def save_validated_matches(text_id, content):
  out_dir = os.path.join('tmp', 'validated')
  make_dirs(out_dir)
  with open(os.path.join(out_dir, text_id), 'a') as out:
    out.write(content)

##
# Cluster Matches
##

def cluster_all_matches():
  pool = Pool(config['max_cores'])
  validated_files = glob.glob(os.path.join('tmp', 'validated', '*'))
  for c, _ in enumerate(pool.imap(cluster_file_matches, validated_files)):
    print(' * clustered matches in', c+1, 'of', len(validated_files), 'files')
  pool.close()
  pool.join()

def cluster_file_matches(validated_file):
  '''
  validated_file is the path to a file in which the filename is a file id
  and the content is a sequence of values with the format:
  fid.sid-fid'.sid'|similarity#. Each of these values represents a validated
  match. Parse out each match value into a dictionary with the shape
  d[match_file_id] = [(int(file_segment_id), int(match_segment_id), float(sim))]
  then format the matches and save in mongo.
  '''
  text_id_a = int( os.path.basename(validated_file) )
  validated_matches = get_validated_matches(validated_file)
  if not validated_matches: return None
  for text_id_b in validated_matches:
    text_id_b = int(text_id_b)
    clusters = cluster(validated_matches[text_id_b])
    format_matches(text_id_a, text_id_b, clusters)

def get_validated_matches(validated_file):
  '''
  validated file is a path to a file on disk with data in the form:
  fid.sid-fid'.sid'|similarity#. Parse out each observation and return
  in a dictionary with the form: d[match_file_id] =
  [(int(file_segment_id), int(match_segment_id), float(similarity))]
  '''
  d = defaultdict(list)
  with open(validated_file) as f:
    for i in f.read().split('#')[:-1]:
      if not i: return None
      ids, sim = i.split('|')
      file_ids, match_ids = ids.split('-')
      file_id, file_segment_id = [int(j) for j in file_ids.split('.')]
      match_file_id, match_segment_id = [int(j) for j in match_ids.split('.')]
      d[match_file_id].append(( file_segment_id, match_segment_id, float(sim) ))
  return d

def cluster(l):
  '''
  Given a list of three-element iterables (source_id, target_id, sim),
  find all sequences of values where s+1 and/or t+1 are matches.
  Using the sim values in each iterable, compute the mean sim for each
  passage cluster.
  '''
  a = [w[0] for w in l]
  b = [w[1] for w in l]
  d = nested(l)

  clusters = []
  for i in sequences(a):
    for j in sequences(b):
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

def sequences(l):
  '''
  Given list `l`, return a list of lists where each sublist
  contains a maximally-long sequence of integers in l
  '''
  sequences = []
  for i in sorted( list( set(l) ) ):
    # check if each is 1 more than the last, as segment ids increment by 1
    if not sequences or sequences[-1][-1] != i-1:
      sequences.append([])
    sequences[-1].append(i)
  return sequences

def nested(l):
  '''
  For each (int_a, int_b, sim) tuple in `l`, add keys to a dict:
  d[ int_a ][ int_b ] = sim for fast lookup of valid int pairs
  '''
  d = defaultdict(lambda: defaultdict())
  for i in l:
    d[ i[0] ][ i[1] ] = i[2]
  return d

##
# Format Matches
##

def format_matches(file_id_a, file_id_b, clusters):
  '''
  Given two file ids and `clusters` -- [{a: [1,2], b: [3,4,5]}, {}...]
  where a's values indicate a sequence of segment ids in a that cluster
  with the sequence of b's values -- format the matches into the required
  structure and save in mongo.
  '''
  db = MongoClient()['intertext']
  text_id_to_path = get_text_id_to_path()
  a_path = text_id_to_path[ int(file_id_a) ]
  b_path = text_id_to_path[ int(file_id_b) ]
  a_file = os.path.basename(a_path)
  b_file = os.path.basename(b_path)
  a_meta = metadata[a_file]
  b_meta = metadata[b_file]
  a_words = open(a_path).read().split()
  b_words = open(b_path).read().split()
  formatted = []
  for c in clusters:
    a_strings = get_match_strings(a_words, c['a'])
    b_strings = get_match_strings(b_words, c['b'])
    a_year = get_value(a_meta, 'year')
    b_year = get_value(b_meta, 'year')
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
        'source_author': get_value(a_meta, 'author'),
        'target_author': get_value(b_meta, 'author'),
        'source_title': get_value(a_meta, 'title'),
        'target_title': get_value(b_meta, 'title'),
        'source_url': get_value(a_meta, 'url'),
        'target_url': get_value(b_meta, 'url'),
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
        'source_year': get_value(b_meta, 'year'),
        'target_year': get_value(a_meta, 'year'),
        'source_author': get_value(b_meta, 'author'),
        'target_author': get_value(a_meta, 'author'),
        'source_title': get_value(b_meta, 'title'),
        'target_title': get_value(a_meta, 'title'),
        'source_url': get_value(b_meta, 'url'),
        'target_url': get_value(a_meta, 'url')
      })
  db.matches.insert_many(formatted)

def get_value(d, k):
  try:
    return d[k]
  except KeyError:
    return ''

def get_text_id_to_path():
  d = {}
  for c, i in enumerate(infiles):
    d[c] = i
  return d

def get_match_strings(words, segment_ids):
  start = min(segment_ids) * config['step']
  end = max(segment_ids) * config['step'] + config['window_size']
  return {
    'prematch': ' '.join(words[max(0, start-config['window_size']):start]),
    'match': ' '.join(words[start:end]),
    'postmatch': ' '.join(words[end:end + config['window_size']])
  }

##
# Create other collections
##

def create_typeahead_collection():
  db = MongoClient()['intertext']
  vals = []
  for i in ['source', 'target']:
    for j in ['author', 'title']:
      for k in db.matches.distinct(i + '_' + j):
        vals.append({'type': i + '_' + j, 'field': j, 'value': k})
  db.typeahead.insert_many(vals)

def create_config_collection():
  db = MongoClient()['intertext']
  db.config.insert(config)

def create_metadata_collection():
  db = MongoClient()['intertext']
  vals = []
  for c, i in enumerate(infiles):
    vals.append({
      'filename': os.path.basename(i),
      'file_id': c,
      'path': i,
      'metadata': metadata[ os.path.basename(i) ]
    })
  db.metadata.insert(vals)

##
# Metadata Helpers
##

def get_metadata():
  print(' * loading metadata')
  path = config['metadata']
  metadata = json.load( open(config['metadata']) )
  # add any missing files to the metadata
  for c, i in enumerate(infiles):
    if not os.path.basename(i) in metadata:
      print(' * warning -', i, 'is missing from metadata keys')
      metadata[ os.path.basename(i) ] = {}
      for j in ['author', 'url', 'image', 'title', 'year']:
        metadata[ os.path.basename(i) ][j] = str(c)
  return metadata

##
# Config Helpers
##

def get_config():
  defaults = {'load_hashbands': False}
  with open('config.json') as f:
    config = json.load(f)
  for k in defaults:
    if not k in config:
      config[k] = defaults[k]
  return config

##
# Main
##

def main():
  if not config['load_hashbands']: count_hashbands()
  match_minhash_keys()
  validate_all_matches()
  cluster_all_matches()
  create_typeahead_collection()
  create_config_collection()
  create_metadata_collection()

if __name__ == '__main__':

  config = get_config()
  infiles = glob.glob(config['infiles'])
  text_ids = [str(i) for i in range(len(infiles))]
  metadata = get_metadata()

  # validate inputs are present
  if not infiles: raise Exception('No input files were found!')

  # remove all extant records
  db = MongoClient()['intertext']
  [db[c].drop() for c in db.collection_names()]

  main()