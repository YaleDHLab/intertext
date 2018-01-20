from __future__ import division
from multiprocessing import Pool
from collections import defaultdict
from datasketch import MinHash
from difflib import SequenceMatcher
from pymongo import MongoClient
from redis import StrictRedis
from nltk import ngrams
from bs4 import BeautifulSoup
import glob, json, sys, os

##
# Minhash
##

def minhash_texts():
  text_id_tuples = ((c, i) for c, i in enumerate(infiles))
  pool = Pool(config['max_cores'])
  for c, _ in enumerate(pool.imap(minhash_text, text_id_tuples)):
    print(' * minhashed', c+1, 'of', len(infiles), 'texts')
  pool.close()
  pool.join()

def minhash_text(text_id_tuple):
  '''
  For each window of words in each input text, compute several
  minhashes for each sequence of 3 characters in that window.
  Combine `config[hashband_length]` of these minhashes into
  a hashband (hash.hash.hash...), and add file_id.segment_offset
  to the list of segments with the given hashband.
  '''
  pipe = r.pipeline()
  text_id, text_path = text_id_tuple
  with open(text_path) as f:
    words = get_text_content(f.read()).lower().split()
    for window_id, window in enumerate(ngrams(words, config['window_size'])):
      if window_id % config['step'] != 0:
        continue
      minhash = MinHash(num_perm=config['n_permutations'], seed=1)
      passage_id = str(text_id) + '.' + str(window_id)
      for ngram in set(ngrams(' '.join(window), 3)):
        minhash.update( ''.join(ngram).encode('utf8') )
      hashband_vals = []
      for i in minhash.hashvalues:
        hashband_vals.append(i)
        if len(hashband_vals) == config['hashband_length']:
          hashband = '.'.join([str(j) for j in hashband_vals])
          hashband_vals = []
          pipe.sadd('minhash-' + hashband, passage_id)
          if len(pipe) >= 10000:
            pipe.execute()
            pipe = r.pipeline()
  pipe.execute()

def get_text_content(s):
  if config['xml_tag']:
    parser = 'html.parser' if infiles[0].split('.')[-1] == '.html' else 'lxml'
    soup = BeautifulSoup(s, parser).find(config['xml_tag'])
    return soup.get_text()
  return s

def map_hashes_to_id_pairs():
  minhash_keys = r.keys('minhash-*')
  pool = Pool(config['max_cores'])
  for c, _ in enumerate(pool.imap(map_hash_to_id_pairs, minhash_keys)):
    c += 1
    if c % 1000 == 0:
      print(' * mapped', c, 'of', len(minhash_keys), 'key ranges')
  pool.close()
  pool.join()

def map_hash_to_id_pairs(minhash_key):
  '''
  Find all keys in Redis that contain minhash-*. Those keys have the
  form minhash-int.int, where each int is a minhash of a segment.
  Those keys have values with the form int_a.int_b, where int_a is
  a file id and int_b is a segment offset denoting a file with the
  given minhash. For each minhash-* key with multiple values,
  find all combinations of those values, and for each, if the
  combination does not have the same file id, add the minhash-*
  key to the set of minhash bands shared by this file id pair.
  '''
  pipe = r.pipeline()
  values = r.smembers(minhash_key)
  if len(values) > 1:
    for id_pair in ngrams(values, 2):
      if id_pair[0].split('.')[0] != id_pair[1].split('.')[0]:
        id_pair_key = 'id-pair-' + '-'.join([i for i in id_pair])
        pipe.sadd(id_pair_key, minhash_key)
  pipe.execute()

def count_all_matches():
  pool = Pool(config['max_cores'])
  id_pairs = r.keys('id-pair-*')
  for c, _ in enumerate(pool.imap(count_matches, id_pairs)):
    c += 1
    if c % 1000 == 0:
      print(' * completed', c, 'of', len(id_pairs), 'match pairs')
  pool.close()
  pool.join()

def count_matches(id_pair):
  '''
  Count the total number of matches between a file id pair,
  and add that file id pair to the set of all files with the given
  number of matches.
  '''
  matches = r.smembers(id_pair)
  r.sadd('matches-' + str(len(matches)), id_pair)

##
# Find Matches
##

def get_text_ids():
  '''
  Store each match between files in a set of matches assigned to the
  higher file id (to minimize i/o during the string compare step.
  '''
  pipe = r.pipeline()
  for i in r.keys('matches-*'):
    for match in r.smembers(i):
      texts = match.replace('id-pair-','').split('-')
      id_one, _ = texts[0].split('.')
      id_two, _ = texts[1].split('.')
      if id_one > id_two:
        pipe.sadd('text-matches-' + id_one, texts[0] + '-' + texts[1])
      elif id_two > id_one:
        pipe.sadd('text-matches-' + id_two, texts[1] + '-' + texts[0])
      if len(pipe) >= 10000:
        pipe.execute()
        pipe = r.pipeline()
  if len(pipe):
    pipe.execute()

def find_all_text_matches():
  pool = Pool(config['max_cores'])
  for result in pool.imap(find_text_matches, text_ids):
    pass
  pool.close()
  pool.join()

def find_text_matches(text_id):
  '''
  For each `text_id`, build a dictionary with matching text ids as keys
  and match segment ids as values. Then for each matching text, compare
  the match segment pairs from `text_id` and the matching text to assess
  similarity. If the similarity is greater than `config.min_similarity`,
  add segment_id.match_segment_id to the set of values assigned to the
  true-matches-file_id-match_file_id key.
  '''
  d = defaultdict(list) # d[text_id] = [(window_id, match_window_id)]
  for i in r.smembers('text-matches-' + text_id):
    texts = i.split('-')
    id_one, window_one = texts[0].split('.')
    id_two, window_two = texts[1].split('.')
    if text_id == id_one:
      d[id_two].append((window_one, window_two))
    else:
      d[id_one].append((window_two, window_one))

  pipe = r.pipeline()
  text_grams = get_text_segments(text_id)
  for match_text_id in d:
    match_grams = get_text_segments(match_text_id)
    for text_window_id, match_window_id in d[match_text_id]:      
      text_window = ' '.join(text_grams[ int(text_window_id) ])
      match_window = ' '.join(match_grams[ int(match_window_id) ])
      similarity = sim(text_window, match_window)
      if similarity >= config['min_similarity']:
        key = 'true-matches-' + text_id + '.' + match_text_id
        value = str(text_window_id) + '.' + str(match_window_id) + '-' + str(similarity)
        pipe.sadd(key, value)
  pipe.execute()

def get_text_segments(text_id):
  with open(infiles[int(text_id)]) as f:
    words = get_text_content(f.read()).lower().split()
    return list(ngrams(words, config['window_size']))

def sim(a, b):
  return SequenceMatcher(None, a, b, autojunk=False).ratio()

##
# Cluster Matches
##

def cluster_all_matches():
  pool = Pool(config['max_cores'])
  for _ in pool.imap(cluster_file_matches, text_ids):
    pass
  pool.close()
  pool.join()

def cluster_file_matches(text_id_a):
  for i in r.keys('true-matches-' + text_id_a + '.*'):
    text_id_a, text_id_b = [int(j) for j in i.split('-')[-1].split('.')]
    segments = []
    for j in r.smembers(i):
      seg_ids, similarity = j.split('-')
      seg_a, seg_b = [int(k) for k in str(seg_ids).split('.')]
      segments.append([seg_a, seg_b, float(similarity) ])
    clusters = cluster(segments)
    format_matches(text_id_a, text_id_b, clusters)

def cluster(l):
  '''
  Given a list of three-element iterables (source_id, target_id, sim),
  find all sequences of values where s+1 and/or t+1 are matches.
  Using the sim values in each iterable, compute the mean sim for each
  passage cluster.
  '''
  a = [t[0] for t in l]
  b = [t[1] for t in l]
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
    # segment ids don't increment by 1, they increment by config.step
    if not sequences or sequences[-1][-1] != i-config['step']:
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
    a_url = get_url(a_meta)
    b_url = get_url(b_meta)
    # identify the file published first as the 'source' file
    if a_meta['year'] < b_meta['year']:
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
        'source_year': a_meta['year'],
        'target_year': b_meta['year'],
        'source_author': a_meta['author'],
        'target_author': b_meta['author'],
        'source_title': a_meta['title'],
        'target_title': b_meta['title'],
        'source_prematch': a_strings['prematch'],
        'target_prematch': b_strings['prematch'],
        'source_match': a_strings['match'],
        'target_match': b_strings['match'],
        'source_postmatch': a_strings['postmatch'],
        'target_postmatch': b_strings['postmatch'],
        'source_url': a_url,
        'target_url': b_url,
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
        'source_year': b_meta['year'],
        'target_year': a_meta['year'],
        'source_author': b_meta['author'],
        'target_author': a_meta['author'],
        'source_title': b_meta['title'],
        'target_title': a_meta['title'],
        'source_prematch': b_strings['prematch'],
        'target_prematch': a_strings['prematch'],
        'source_match': b_strings['match'],
        'target_match': a_strings['match'],
        'source_postmatch': b_strings['postmatch'],
        'target_postmatch': a_strings['postmatch'],
        'source_url': b_url,
        'target_url': a_url
      })
  db.matches.insert_many(formatted)

def get_url(metadata):
  try:
    return metadata['url']
  except KeyError:
    return ''

def get_text_id_to_path():
  d = {}
  for c, i in enumerate(infiles):
    d[c] = i
  return d

def get_match_strings(words, segment_ids):
  start = min(segment_ids)
  end = max(segment_ids) + config['window_size']
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
# Main
##

def main():
  minhash_texts()
  map_hashes_to_id_pairs()
  count_all_matches()
  get_text_ids()
  find_all_text_matches()
  cluster_all_matches()
  create_typeahead_collection()
  create_config_collection()
  create_metadata_collection()

if __name__ == '__main__':
  with open('config.json') as f:
    config = json.load(f)

  r = StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
  infiles = glob.glob(config['infiles'])
  text_ids = [str(i) for i in range(len(infiles))]
  metadata = json.load( open(config['metadata']) )

  # validate inputs are present
  if not infiles: raise Exception('No input files were found!')

  # conditionally remove all extant records
  db = MongoClient()['intertext']
  if config['flushall']:
    r.flushall()
    [db[c].drop() for c in db.collection_names()]
  del db

  main()