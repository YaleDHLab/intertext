from datasketch import MinHash, MinHashLSH
from collections import defaultdict
from difflib import SequenceMatcher
from nltk import ngrams
import functools
import argparse
import codecs
import glob
import json
import os

config = {
  'infiles': [],
  'encoding': 'utf8',
  'window_length': 14,
  'slide_length': 4,
  'permutations': 256,
  'threshold': 0.5,
  'similarity': 0.7,
  'recall': 0.9,
}

def parse():
  '''Parse the command line arguments and initialize text processing'''
  description = 'Discover and visualize text reuse'
  parser = argparse.ArgumentParser(description=description, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--infiles', '-i', type=str, default=config['infiles'], help='path to a glob of text files to process', required=True)
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
  # identify and store infiles
  infiles = glob.glob(kwargs['infiles'])
  if not os.path.exists('output'):
    os.makedirs('output')
  with open(os.path.join('output', 'files.json'), 'w') as out:
    json.dump(infiles, out)
  # create minhashes
  print(' * creating minhashes')
  id_d = {} # d[window_id] = [file_id, window_index]
  minhash_d = {} # d[window_id] = minhash
  n = 0 # unique id for each window
  for file_idx, i in enumerate(infiles):
    for window_idx, window in enumerate(get_windows(i, **kwargs)):
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
    file_a_windows = list(get_windows(infiles[file_id_a], **kwargs))
    for file_id_b in candidate_d[file_id_a]:
      file_b_windows = list(get_windows(infiles[file_id_b], **kwargs))
      for window_a, window_b in candidate_d[file_id_a][file_id_b]:
        text_a = file_a_windows[window_a]
        text_b = file_b_windows[window_b]
        sim = SequenceMatcher(None, text_a, text_b, autojunk=False).ratio()
        if sim > kwargs['similarity']:
          matches_d[file_id_a][file_id_b].append([window_a, window_b, round(sim, 2)])
  del candidate_d
  # cluster the matches
  print(' * clustering matches')
  for file_id_a in matches_d:
    for file_id_b in matches_d[file_id_a]:
      window_as, window_bs, sims = zip(*matches_d[file_id_a][file_id_b])
      # find sequences of windows in a and b; resume from get_clusters() in original


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
def get_windows(path, **kwargs):
  '''Given a file path return a list of strings from that file'''
  with codecs.open(path, 'r', kwargs['encoding']) as f:
    f = f.read()
  words = f.split()
  l = []
  for idx, window in enumerate(list(ngrams(words, kwargs['window_length']))):
    if idx % kwargs['slide_length'] != 0:
      continue
    l.append(' '.join(window))
  return l

if __name__ == '__main__':
  parse()
