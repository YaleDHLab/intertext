const _ = require('lodash')
const models = require('../models/models')
const queryParser = require('../lib/queryParser')

module.exports = (app) => {

  app.get('/api', (req, res, next) => {
    res.json({
      'avialable_routes': [
        '/clustered_matches',
        '/config',
        '/matches',
        '/metadata',
        '/minhash_matches',
        '/segments/:file_id',
        '/segments_to_pages/:file_id',
        '/texts/:file_id',
        '/files',
        '/typeahead_values'
      ]
    })
  })

  app.get('/api/config', (req, res, next) => {
    models.config.find({}, (err, data) => {
      if (err) console.warn(err);
      res.json(data);
    })
  })

  app.get('/api/clustered_matches', (req, res, next) => {
    var query = {}
    var pagination = {limit: 100, offset: 0, select: {'_id': 0}}

    /**
    * Fields that necessitate full text search
    **/

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

    /**
    * Fields that are only passed as simple key value queries
    * where the value should be parsed as an integer
    **/

    var keyValueInts = [
      'source_file_id',
      'target_file_id',
      'source_segment_id',
      'target_segment_id',
      'match_id'
    ]

    Object.keys(req.query).map((f) => {
      if (_.includes(keyValueInts, f)) {
        query[f] = parseInt(req.query[f])
      }
    })

    /**
    * Fields that specify a list of values, only one of which needs to
    * be present in any match
    **/

    var inFields = {
      'source_segment_ids': 'source_segment_ids',
      'target_segment_ids': 'target_Segment_ids'
    }

    Object.keys(req.query).map((f) => {
      if (Object.keys(inFields).indexOf(f) > -1) {
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

    /**
    * Fields that specify a list of values, all of which need to be present
    * for a match to occur
    **/

    listFields = ['match_ids'];

    listFields.map((f) => {
      if (req.query[f]) {
        query[f] = {$all: req.query[f]}
      }
    })

    /**
    * Similarity search
    **/

    if (req.query.min_similarity && req.query.max_similarity) {
      query['similarity'] = {
        $gte: parseInt(req.query.min_similarity)/100,
        $lte: parseInt(req.query.max_similarity)/100
      }
    }

    /**
    * Response sort order
    *
    * The fallback sort on match_cluster_id's ensures consistent ordering
    * in compare views in cases where the user hasn't selected a sort option
    **/

    if (req.query.sort) {
      var sort = {};
      sort[req.query.sort] = req.query.sort == 'similarity' ? -1 : 1;
      pagination.sort = sort;
    } else {
      pagination.sort = {'match_cluster_id': 1}
    }

    /**
    * Limit and offset
    **/

    ['limit', 'offset'].map((p) => {
      if (req.query[p]) {
        pagination[p] = parseInt(req.query[p]);
      }
    })

    /**
    * Select based search to limit response fields
    **/

    if (req.query.select) {
      req.query.select.split(' ').map((f) => {
        pagination.select[f] = 1;
      })
    }

    /**
    * Chart query: find matches where file is source or target
    **/

    if (req.query.source_or_target_file_id) {
      query = {
        $or:[
          {'source_file_id': parseInt(req.query.source_or_target_file_id)},
          {'target_file_id': parseInt(req.query.source_or_target_file_id)}
        ]
      }
    }

    /**
    * Send paginated results to application queries
    **/

    models.clustered_match.paginate(query, pagination, (err, data) => {
      if (err) {console.warn(err)};
      res.json(data);
    })
  })

  app.get('/api/matches', (req, res, next) => {
    var query = {}
    var pagination = {limit: 50, offset: 0, select: {'_id': 0}}

    /**
    * Fields that necessitate full text search
    **/

    var fulltextSearchFields = [
      'author',
      'title'
    ]

    /**
    * Fields that are only passed as simple key value queries
    * where the value should be parsed as an integer
    **/

    var keyValueInts = [
      'source_file_id',
      'target_file_id',
      'source_segment_id',
      'target_segment_id',
      'match_id'
    ]

    Object.keys(req.query).map((f) => {
      var fieldSuffix = f.split('_')[1];
      if (fulltextSearchFields.indexOf(fieldSuffix) != -1) {
        query[f] = {$regex: queryParser.regexEscape(req.query[f])};
      }

      if (keyValueInts.indexOf(f) != -1) {
        query[f] = parseInt(req.query[f]);
      }
    })

    /**
    * Similarity search
    **/

    if (req.query.min_similarity && req.query.max_similarity) {
      query['similarity'] = {
        $gte: parseInt(req.query.min_similarity)/100,
        $lte: parseInt(req.query.max_similarity)/100
      }
    }

    /**
    * Response sort order
    **/

    if (req.query.sort) {
      pagination.sort = req.query.sort;
    }

    /**
    * Limit and offset
    **/

    ['limit', 'offset'].map((p) => {
      if (req.query[p]) {
        pagination[p] = parseInt(req.query[p]);
      }
    })

    /**
    * Select based search to limit response fields
    **/

    if (req.query.select) {
      req.query.select.split(' ').map((f) => {
        pagination.select[f] = 1;
      })
    }

    /**
    * Chart query: find matches where file is source or target
    **/

    if (req.query.source_or_target_file_id) {
      query = {
        $or:[
          {'source_file_id': parseInt(req.query.source_or_target_file_id)},
          {'target_file_id': parseInt(req.query.source_or_target_file_id)}
        ]
      }
    }

    /**
    * Send only distinct fields; for typeahead queries
    **/

    if (req.query.distinct) {
      models.match.distinct(req.query.distinct, query, (err, data) => {
        if (err) {console.warn(err)};
        res.json(data);
      })

    /**
    * Send paginated results to application queries
    **/

    } else {
      models.match.paginate(query, pagination, (err, data) => {
        if (err) {console.warn(err)};
        res.json(data);
      })
    }
  })

  app.get('/api/typeahead_values', (req, res, next) => {
    var query = {};

    var stringFields = [
      'field',
    ]

    stringFields.map((f) => {
      if (req.query[f]) {
        query[f] = req.query[f]
      }
    })

    var listFields = [
      'type'
    ]

    // there are only two levels for the type field, so if the query
    // contains an array of elements for the type field, we can drop
    // the field from the query altogether
    listFields.map((f) => {
      if (req.query[f] && !Array.isArray(req.query[f])) {
        query[f] = req.query[f];
      }
    })

    var fulltextFields = [
      'value'
    ]

    fulltextFields.map((f) => {
      if (req.query[f]) {
        query[f] = {
          $regex: queryParser.regexEscape(req.query[f]),
          $options : 'i'
        }
      }
    })

    models.typeahead_values.distinct('value', query, (err, data) => {
      if (err) {console.warn(err)};
      res.json(data);
    })
  })

  app.get('/api/minhash_matches', (req, res, next) => {
    var query = {}
    var pagination = {limit: 50, offset: 0, select: {'_id': 0}}

    var keyValueInts = [
      'file_id',
      'match_file_id'
    ]

    if (req.query) {
      Object.keys(req.query).map((f) => {
        if (keyValueInts.indexOf(f) != -1) {
          query[f] = parseInt(req.query[f]);
        }
      })
    }

    ['limit', 'offset'].map((p) => {
      if (req.query[p]) {
        pagination[p] = parseInt(req.query[p]);
      }
    })

    models.minhash_match.paginate(query, pagination, (err, data) => {
      if (err) {console.warn(err)};
      res.json(data);
    })
  })

  app.get('/api/metadata', (req, res, next) => {
    var query = {}
    var pagination = {limit: 50, offset: 0, select: {'_id': 0}}

    var keyValueInts = [
      'file_id'
    ]

    if (req.query) {
      Object.keys(req.query).map((f) => {
        if (keyValueInts.indexOf(f) != -1) {
          query[f] = parseInt(req.query[f]);
        }
      })
    }

    ['limit', 'offset'].map((p) => {
      if (req.query[p]) {
        pagination[p] = parseInt(req.query[p]);
      }
    })

    models.metadata.paginate(query, pagination, (err, data) => {
      if (err) {console.warn(err)};
      res.json(data);
    })
  })

  app.route('/api/segments/:file_id', (req, res, next) => {
    if (req.params.file_id) {
      var query = {file_id: parseInt(req.params.file_id)}
      models.segment.find(query, (err, data) => {
        if (err) {console.warn(err)};
        if (req.query.segment_id) {
          res.json({
            'segmentCount': data[0].segments.length,
            'segment': data[0].segments[parseInt(req.query.segment_id)]
          })
        } else {
          res.json(data)
        }
      })
    }
  })

  app.get('/api/segments_to_pages/:file_id', (req, res, next) => {
    if (req.params.file_id) {
      var query = {file_id: parseInt(req.params.file_id)}
      models.segment_to_page.find(query, (err, data) => {
        if (err) {console.warn(err)};
        res.json(data);
      })
    }
  })

  app.get('/api/texts/:file_id', (req, res, next) => {
    if (req.params.file_id) {
      var query = {file_id: parseInt(req.params.file_id)}
      models.text.find(query, (err, data) => {
        if (err) {console.warn(err)};
        res.json(data);
      })
    }
  })

  /**
  * Raw text request routes
  **/

  app.get('/api/files', (req, res, next) => {
    if (req.query.file_path) {
      var splitPath = req.query.file_path.split('/');
      var dirs = [];
      var filename = ''
      splitPath.map((p, i) => {
        i + 1 == splitPath.length ? filename = p : dirs.push(p)
      })
      res.sendFile(path.join(__dirname, dirs.join('/'), filename))
    } else {
      res.json([
        'please query for a file_path relative to the location of server.js',
        'e.g. ?file_path=/utils/data/plagiary_poets/text/34360.txt'
      ])
    }
  })

  return app;
}