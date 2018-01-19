const _ = require('lodash')
const path = require('path')
const models = require('../models/models')
const queryParser = require('../lib/queryParser')

module.exports = (app) => {

  app.get('/api', (req, res, next) => {
    res.json({
      'avialable_routes': [
        '/config',
        '/matches',
        '/metadata',
        '/typeahead',
        '/files',
      ]
    })
  })

  app.get('/api/config', (req, res, next) => {
    models.config.find({}, (err, data) => {
      if (err) console.warn(err);
      res.json(data);
    })
  })

  app.get('/api/matches', (req, res, next) => {
    var query = {}
    var pagination = {limit: 100, offset: 0}

    // Fields that necessitate full text search
    var fulltextSearchFields = {
      'author': ['source_author', 'target_author'],
      'title': ['source_title', 'target_title']
    }

    _.keys(fulltextSearchFields).map((field) => {
      var fields = fulltextSearchFields[field];

      // case where user wants to run an and or query
      if (_.difference(fields, _.keys(req.query)).length === 0) {
        var options = [];
        fields.map((f) => {
          var option = {};
          option[f] = {$regex: queryParser.regexEscape(req.query[f])};
          options.push(option)
        })
        query = {$or: options}

      // case where user wants to query for a single field
      } else {
        fields.map((f) => {
          if (_.includes(_.keys(req.query), f)) {
            query[f] = {$regex: queryParser.regexEscape(req.query[f])};
          }
        })
      }
    })

    // Queries by Mongo id
    if (req.query['_id']) query['_id'] = req.query['_id'];

    // Queries with these fields should pass ints as args
    var keyValueInts = [
      'source_file_id',
      'target_file_id',
      'source_segment_id',
      'target_segment_id',
    ]

    _.keys(req.query).map((f) => {
      if (_.includes(keyValueInts, f)) query[f] = parseInt(req.query[f])
    })

    // Fields that specify a list of args, only one needs to be present for a hit
    var inFields = {
      'source_segment_ids': 'source_segment_ids',
      'target_segment_ids': 'target_Segment_ids'
    }

    _.keys(req.query).map((f) => {
      if (_.keys(inFields).indexOf(f) > -1) {
        // case where there are multiple values for the field
        if (Array.isArray(req.query[f])) {
          var arr = [];
          req.query[f].map((val) => {
            arr.push(parseInt(val))
          });
          query[f] = {$in: arr}

        // case where there's one value for the field
        } else {
          query[f] = {$in: [parseInt(req.query[f])]}
        }
      }
    })

    // Similarity search
    if (req.query.min_similarity && req.query.max_similarity) {
      query['similarity'] = {
        $gte: parseInt(req.query.min_similarity)/100,
        $lte: parseInt(req.query.max_similarity)/100
      }
    }

    // Sort order - fallback to match_cluster_ids ensures consistent ordering
    // in compare views if users haven't chosen a sort option
    if (req.query.sort) {
      var sort = {};
      sort[req.query.sort] = req.query.sort == 'similarity' ? -1 : 1;
      pagination.sort = sort;
    } else {
      pagination.sort = {'match_cluster_id': 1}
    }

    // Limit and offset
    ['limit', 'offset'].map((p) => {
      if (req.query[p]) pagination[p] = parseInt(req.query[p]);
    })

    // Select based search to limit response fields
    if (req.query.select) {
      req.query.select.split(' ').map((f) => pagination.select[f] = 1)
    }

    // Chart query: find matches where file is source or target
    if (req.query.source_or_target_file_id) {
      query = {
        $or:[
          {'source_file_id': parseInt(req.query.source_or_target_file_id)},
          {'target_file_id': parseInt(req.query.source_or_target_file_id)}
        ]
      }
    }

    // Send paginated results to application queries
    models.match.paginate(query, pagination, (err, data) => {
      if (err) {console.warn(err)};
      res.json(data);
    })
  })

  /**
  * Typeahead request routes
  **/

  app.get('/api/typeahead', (req, res, next) => {
    var query = {};

    if (req.query['field']) query['field'] = req.query['field']

    // type type field has two levels, so don't filter if given an arg array
    if (req.query['type'] && !Array.isArray(req.query['type'])) {
      query['type'] = req.query['type'];
    }

    if (req.query['value']) {
      query['value'] = {
        $regex: queryParser.regexEscape(req.query['value']),
        $options : 'i'
      }
    }

    models.typeahead.distinct('value', query, (err, data) => {
      if (err) {console.warn(err)};
      res.json(data);
    })
  })

  /**
  * Metadata request routes
  **/

  app.get('/api/metadata', (req, res, next) => {
    var query = {}
    var pagination = {limit: 50, offset: 0, select: {'_id': 0}}

    if (req.query) {
      _.keys(req.query).map((f) => {
        if (f === 'file_id') query[f] = parseInt(req.query[f])
      })
    }

    ['limit', 'offset'].map((p) => {
      if (req.query[p]) pagination[p] = parseInt(req.query[p])
    })

    models.metadata.paginate(query, pagination, (err, data) => {
      if (err) {console.warn(err)};
      res.json(data);
    })
  })

  /**
  * Raw text request routes
  **/

  app.get('/api/files', (req, res, next) => {
    if (req.query.file_path) {
      res.sendFile(path.join(__dirname, '..', '..', req.query.file_path))
    } else {
      res.json([
        'please query for a file_path relative to the location of server.js',
        'e.g. ?file_path=/data/texts/34360.txt'
      ])
    }
  })

  return app;
}