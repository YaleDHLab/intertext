from datasketch import MinHash, MinHashLSH
from collections import defaultdict
from difflib import SequenceMatcher
from nltk import ngrams
import functools
import argparse
import codecs
import shutil
import glob
import json
import os

config = {
  'infile_glob': [],
  'metadata': '',
  'encoding': 'utf8',
  'window_length': 14,
  'slide_length': 4,
  'permutations': 256,
  'threshold': 0.5,
  'similarity': 0.5,
  'recall': 0.95,
}

'''
TODO:
  * add max similarity to other files to disregard direct file copying
  * add xml parsing (tag to parse, tags to exclude)
  * add files from which matches should be blacklisted
  * add flag to indicate if same-author matches are allowed
  * add support for CSV metadata
  * add removal of diacritics
  * add support for xml + txt in same run
  * add unique guid for each output set to avoid overwriting
'''

def parse():
  '''Parse the command line arguments and initialize text processing'''
  description = 'Discover and visualize text reuse'
  parser = argparse.ArgumentParser(description=description, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--infiles', '-i', type=str, default=config['infile_glob'], dest='infile_glob', help='path to a glob of text files to process', required=True)
  parser.add_argument('--metadata', '-m', type=str, default=config['metadata'], help='path to a JSON metadata file (see README)', required=True)
  parser.add_argument('--encoding', '-e', type=str, default=config['encoding'], help='the encoding of infiles', required=False)
  parser.add_argument('--window_length', '-w', type=int, default=config['window_length'], help='the length of windows when processing files (see README)', required=False)
  parser.add_argument('--slide_length', '-l', type=int, default=config['slide_length'], help='the length to slide windows when processing files (see README)', required=False)
  parser.add_argument('--permutations', '-p', type=int, default=config['permutations'], help='the number of permutation functions to use (see README)', required=False)
  parser.add_argument('--threshold', '-t', type=int, default=config['threshold'], help='the minhash threshold value (see README)', required=False)
  parser.add_argument('--similarity', '-s', type=int, default=config['similarity'], help='the minimum similarity of matches to retain)', required=False)
  parser.add_argument('--recall', '-r', type=int, default=config['recall'], help='the recall value to aim for when discovering matches', required=False)
  config.update(vars(parser.parse_args()))
  process_texts(**config)

def process_texts(**kwargs):
  '''Process the user's texts using the specified params'''
  lsh = MinHashLSH(threshold=kwargs['threshold'],
    num_perm=kwargs['permutations'],
    weights=(1-kwargs['recall'], kwargs['recall']))
  # create the output directories
  if os.path.exists('output'):
    shutil.rmtree('output')
  if not os.path.exists(os.path.join('output', 'matches')):
    os.makedirs(os.path.join('output', 'matches'))
  # identify and store infiles
  infiles = glob.glob(kwargs['infile_glob'])
  if len(infiles) == 0:
    raise Exception('No infiles could be found!')
  with open(os.path.join('output', 'files.json'), 'w') as out:
    json.dump(infiles, out)
  # if the user provided metadata, store it in the kwargs
  if kwargs.get('metadata'):
    kwargs['metadata'] = json.load(open(kwargs['metadata']))
  # create minhashes
  print(' * creating minhashes')
  id_d = {} # d[window_id] = [file_id, window_index]
  minhash_d = {} # d[window_id] = minhash
  n = 0 # unique id for each window
  for file_idx, i in enumerate(infiles):
    for window_idx, window in enumerate(get_windows(i, **get_cacheable(kwargs))):
      id_d[n] = [file_idx, window_idx]
      m = MinHash(num_perm=kwargs['permutations'])
      for w in ngrams(window, 3):
        m.update(''.join(w).encode('utf-8'))
      minhash_d[n] = m
      lsh.insert(n, m)
      n += 1
  # create the graph of all match candidates
  print(' * identifying match candidates')
  candidate_d = defaultdict(lambda: defaultdict(list))
  for i in range(n):
    file_id_a, window_index_a = id_d[i]
    for j in lsh.query(minhash_d[i]):
      file_id_b, window_index_b = id_d[j]
      if file_id_a < file_id_b:
        candidate_d[file_id_a][file_id_b].append([window_index_a, window_index_b])
      else:
        candidate_d[file_id_b][file_id_a].append([window_index_b, window_index_a])
  del lsh
  del minhash_d
  # validate matches from among the candidates
  print(' * validating matches')
  matches_d = defaultdict(lambda: defaultdict(list))
  for file_id_a in candidate_d:
    file_a_windows = list(get_windows(infiles[file_id_a], **get_cacheable(kwargs)))
    for file_id_b in candidate_d[file_id_a]:
      file_b_windows = list(get_windows(infiles[file_id_b], **get_cacheable(kwargs)))
      for window_a, window_b in candidate_d[file_id_a][file_id_b]:
        text_a = file_a_windows[window_a]
        text_b = file_b_windows[window_b]
        sim = SequenceMatcher(None, text_a, text_b, autojunk=False).ratio()
        if sim > kwargs['similarity']:
          matches_d[file_id_a][file_id_b].append([window_a, window_b, round(sim, 2)])
  del candidate_d
  # cluster the matches
  print(' * clustering matches')
  formatted = []
  for file_id_a in matches_d:
    for file_id_b in matches_d[file_id_a]:
      clusters = []
      # create d[file_a_window][file_b_window] = sim
      d = defaultdict(lambda: defaultdict())
      for a, b, sim in matches_d[file_id_a][file_id_b]:
        d[a][b] = sim
      # find sequences of windows in a and b
      window_as, window_bs, sims = zip(*matches_d[file_id_a][file_id_b])
      for a in get_sequences(window_as):
        for b in get_sequences(window_bs):
          cluster = {'a': [], 'b': [], 'sim': []}
          for a_i in a:
            for b_i in b:
              try:
                sim = d[a_i][b_i]
                if not sim: continue
                cluster['a'].append(a_i)
                cluster['b'].append(b_i)
                cluster['sim'].append(sim)
              except KeyError:
                pass
          if cluster['a'] and cluster['b']:
            clusters.append({
              'a': sorted( list( set( cluster['a'] ) ) ),
              'b': sorted( list( set( cluster['b'] ) ) ),
              'sim': round( sum(cluster['sim']) / len(cluster['sim']), 2)
            })
      # format the clusters for the current file pair
      matches = format_matches(file_id_a, file_id_b, clusters, infiles, **kwargs)
      if matches: formatted.append(matches)
  write_outputs(infiles, formatted)

def format_matches(file_id_a, file_id_b, clusters, infiles, **kwargs):
  '''Given integer file ids and clusters [{a: [], b: [], sim: []}] format matches for display'''
  if file_id_a == file_id_b: return
  a_meta = kwargs.get('metadata', {}).get(os.path.basename(infiles[file_id_a]), {})
  b_meta = kwargs.get('metadata', {}).get(os.path.basename(infiles[file_id_b]), {})
  # set a equal to the record pubished first
  if a_meta and b_meta and b_meta.get('year') < a_meta.get('year'):
    old_a = file_id_a
    old_b = file_id_b
    file_id_b = old_a
    file_id_a = old_b
    a_meta = kwargs.get('metadata', {}).get(os.path.basename(infiles[file_id_a]), {})
    b_meta = kwargs.get('metadata', {}).get(os.path.basename(infiles[file_id_b]), {})
  # format the matches
  a_words = get_words(infiles[file_id_a], **get_cacheable(kwargs))
  b_words = get_words(infiles[file_id_b], **get_cacheable(kwargs))
  formatted = []
  for c in clusters:
    a_strings = get_match_strings(a_words, c['a'], **get_cacheable(kwargs))
    b_strings = get_match_strings(b_words, c['b'], **get_cacheable(kwargs))
    formatted.append({
      'similarity': c['sim'],
      'source_file_id': int(file_id_a),
      'target_file_id': int(file_id_b),
      'source_segment_ids': c['a'],
      'target_segment_ids': c['b'],
      'source_filename': os.path.basename(infiles[file_id_a]),
      'target_filename': os.path.basename(infiles[file_id_b]),
      'source_file_path': infiles[file_id_a],
      'target_file_path': infiles[file_id_b],
      'source_prematch': a_strings['prematch'],
      'target_prematch': b_strings['prematch'],
      'source_match': a_strings['match'],
      'target_match': b_strings['match'],
      'source_postmatch': a_strings['postmatch'],
      'target_postmatch': b_strings['postmatch'],
      'source_year': a_meta.get('year', ''),
      'target_year': b_meta.get('year', ''),
      'source_author': a_meta.get('author', ''),
      'target_author': b_meta.get('author', ''),
      'source_title': a_meta.get('title', ''),
      'target_title': b_meta.get('title', ''),
      'source_url': a_meta.get('url', ''),
      'target_url': b_meta.get('url', ''),
    })
  return(formatted)

def get_match_strings(words, window_ids, **kwargs):
  '''Given a list of words and window ids, format prematch, match, and postmatch strings for a match'''
  start = min(window_ids) * kwargs['slide_length']
  end = max(window_ids) * kwargs['slide_length'] + kwargs['window_length']
  return {
    'prematch': ' '.join(words[max(0, start-kwargs['window_length']):start]),
    'match': ' '.join(words[start:end]),
    'postmatch': ' '.join(words[end:end + kwargs['window_length']])
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

@functools.lru_cache(maxsize=128)
def get_words(path, **kwargs):
  '''Given a file path return a list of strings from that file'''
  with codecs.open(path, 'r', kwargs['encoding']) as f:
    f = f.read()
  return f.split()

@functools.lru_cache(maxsize=128)
def get_windows(path, **kwargs):
  '''Given a file path return a list of strings from that file'''
  words = get_words(path, **kwargs)
  l = []
  for idx, window in enumerate(list(ngrams(words, kwargs['window_length']))):
    if idx % kwargs['slide_length'] != 0:
      continue
    l.append(' '.join(window))
  return l

def get_cacheable(kwargs):
  '''Given a dictionary of kwargs return a dictionary with cacheable values retained'''
  d = {}
  for k in kwargs:
    if not isinstance(kwargs[k], list) and not isinstance(kwargs[k], dict):
      d[k] = kwargs[k]
  return d

def write_outputs(infiles, formatted):
  '''Given a 2D list where sublists are matches between two texts, write all outputs'''
  # write the subdirectories if necessary
  for i in range(len(infiles)):
    out = os.path.join('output', 'matches', str(i))
    if not os.path.exists(out):
      os.makedirs(out)
  # create sets that store author and title lists
  authors = defaultdict(set)
  titles = defaultdict(set)
  for idx, i in enumerate(formatted):
    # add the authors and titles to the lists of authors/titles
    authors[i[0]['source_author']].add(i[0]['source_file_id'])
    authors[i[0]['target_author']].add(i[0]['target_file_id'])
    titles[i[0]['source_title']].add(i[0]['source_file_id'])
    titles[i[0]['target_title']].add(i[0]['target_file_id'])
    # write the match into locations from which it can be queried - these will be combined below
    with open(os.path.join('output', 'matches', str(i[0]['source_file_id']), str(idx)), 'w') as out:
      json.dump(i, out)
    with open(os.path.join('output', 'matches', str(i[0]['target_file_id']), str(idx)), 'w') as out:
      json.dump(i, out)
  # write the aggregated authors and titles
  with open(os.path.join('output', 'authors.json'), 'w') as out:
    json.dump({k: list(authors[k]) for k in authors}, out)
  with open(os.path.join('output', 'titles.json'), 'w')  as out:
    json.dump({k: list(titles[k]) for k in titles}, out)
  # combine the files in each of the match directories - loop over types of output (file_id, author, title)
  for i in glob.glob(os.path.join('output', 'matches', '*')):
    file_id = os.path.split(i)[1]
    l = []
    for j in glob.glob(os.path.join(i, '*')):
      with open(j) as f:
        l += json.load(f)
    # remove the uncombined matches
    shutil.rmtree(i)
    # write the combined matches
    with open(os.path.join('output', 'matches', file_id + '.json'), 'w') as out:
      json.dump(l, out)

if __name__ == '__main__':
  parse()