var mongoose = require('mongoose');
var models = require('./models/models');
var config = require('./config');

module.exports = () => {
  mongoose.connect('mongodb://localhost/' + config.db, {
    keepAlive: true,
    reconnectTries: Number.MAX_VALUE,
    useMongoClient: true
  })

  mongoose.connection.on('error', (err) => {
    console.log(err)
  })

  return mongoose;
}