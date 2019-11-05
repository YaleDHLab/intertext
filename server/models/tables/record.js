var config = require('../../config')
var mongoose = require('mongoose')
var db = require('../db')
var table = 'record'

var schema = new mongoose.Schema(db[table])
var capitalized = table.charAt(0).toUpperCase() + table.slice(1)
var model = mongoose.model(capitalized, schema, table)
module.exports = model;