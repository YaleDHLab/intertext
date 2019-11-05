var mongoose = require('mongoose');
var models = require('./models/models');
var config = require('./config');

var mongooseConfig = {
  keepAlive: true,
  useNewUrlParser: true,
  useUnifiedTopology: true,
  reconnectTries: Number.MAX_VALUE,
}

mongoose.connect('mongodb://localhost/' + config.db, mongooseConfig)

mongoose.connection.on('error', (err) => {
  console.log(err)
})

module.exports = {
  connection: mongoose,
  config: mongooseConfig,
}
