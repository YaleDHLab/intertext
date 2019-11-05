var config = require('../config')
var schemas = require('./schemas')
var mongoose = require('mongoose')
var paginate = require('mongoose-paginate')

module.exports = (tableName) => {
  var schema = new mongoose.Schema(schemas[tableName])

  // add pagination
  schema.plugin(paginate)

  /**
  * Before saving, fetch a timestamp and either add
  * a created at field or updated at value
  **/

  if (schemas[tableName].created_at && schemas[tableName].updated_at) {
    schema.pre('save', function(next) {
      var currentDate = new Date();

      // update the updated_at value
      this.updated_at = currentDate;

      // create the created_at value if necessary
      if (!this.created_at) this.created_at = currentDate;

      next()
    })
  }

  var capitalized = tableName.charAt(0).toUpperCase() + tableName.slice(1)
  var model = mongoose.model(capitalized, schema, tableName)
  return model;
}