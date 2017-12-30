module.exports = {
  'clustered_match': require('./table')('clustered_matches'),
  'config': require('./table')('config'),
  'match': require('./table')('matches'),
  'metadata': require('./table')('metadata'),
  'minhash_match': require('./table')('minhash_matches'),
  'segment': require('./table')('segments'),
  'text': require('./table')('texts'),
  'typeahead_values': require('./table')('typeahead_values'),
}