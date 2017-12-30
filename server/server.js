// server.js
var express = require('express')
var path = require('path')
var config = require('./config')

// initialize mongoose
const mongoose = require('./mongo')();

// initialize server
const app = require('./middleware')(express())

// add routes
require('./routes/routes')(app)

// ask server to listen on desired port
const port = process.env.PORT || config.api.port;
app.listen(port, () => {
  console.log('Listening on localhost:' + port)
})