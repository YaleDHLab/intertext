from __future__ import absolute_import
from os.path import join
from glob import glob
from celery import group
import sys
[sys.path.append(i) for i in ['.', '..']]
import intertext.tasks as tasks
import intertext.helpers as helpers

def hash_inputs():
  '''
  For each input file, partition that text into windows. Hash each
  window, then pass a sliding window over the hashes to get a
  sequence of "hashbands" for each window. Store the hashbands
  that occur in each infile.
  '''
  print(' * processing hashbands')
  jobs = group(tasks.hash_input_file.s(i) for i in text_ids)
  for idx, i in enumerate(jobs.apply_async()):
    result = i.get()
    print(' * processed', idx+1, 'of', len(text_ids), 'hashbands')
  # write the remaining hashbands in redis to disk
  helpers.write_redis_hashbands()


def find_candidate_matches():
  '''
  Step 1 generates a map from hashband to the text id and window ids
  in which that hashband occurs. Step 2 uses those output files
  to identify candidate pairs of text matches. This step is
  separated from Step 3, which validates match pairs, because if we
  can identify all of the candidate matches between two files
  before the validation step, we can drastically reduce i/o.
  '''
  dirs = glob(join(config['tmp'], 'hashbands', '*',))
  jobs = group(tasks.find_candidates.s(i) for i in dirs)
  for idx, i in enumerate(jobs.apply_async()):
    result = i.get()
    if (idx + 1) % 100 == 0:
      print(' * processed', idx, 'of', len(dirs), 'hashband files')
  # write the remaining match candidates in redis to disk
  helpers.write_redis_candidates()


def validate_matches():
  '''
  Given the candidate matches identified by Step 2, read in the
  matches for each text pair and validate them with raw string
  similarity lookups
  '''
  match_files = glob(join(config['tmp'], 'candidates', '*'))
  jobs = group(tasks.validate_matches.s(i) for i in match_files)
  for idx, i in enumerate(jobs.apply_async()):
    result = i.get()
    print(' * validated', idx+1, 'of', len(match_files), 'file matches')


def cluster_matches():
  '''
  Cluster neighboring validated matches so that matches from
  consecutive window ids will be combined into a single match
  object.
  '''
  validated_files = glob(join(config['tmp'], 'matches', '*'))
  jobs = group(tasks.cluster_matches.s(i) for i in validated_files)
  for idx, i in enumerate(jobs.apply_async()):
    result = i.get()
    print(' * clustered matches in', idx+1, 'of', len(validated_files), 'files')


if __name__ == '__main__':

  config = helpers.config
  text_ids = helpers.text_ids
  db = helpers.get_db()

  if config['clear_tmp_files']:
    print(' * clearing tmp files')
    helpers.rm_dirs(config['tmp'])

  # remove saved records in mongo
  if config['clear_db']:
    print(' * clearing db')
    [db[c].drop() for c in db.collection_names()]

  if config['start_celery']:
    celery_task = helpers.start_celery()

  # run functions
  hash_inputs()
  find_candidate_matches()
  validate_matches()
  cluster_matches()
  tasks.create_collections()

  # terminate the celery processes if they were started
  if config['start_celery']:
    helpers.terminate_process(celery_task.pid)