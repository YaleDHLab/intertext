var express = require('express')
var methodOverride = require('method-override')
var cookieParser = require('cookie-parser')
var compression = require('compression')
var bodyParser = require('body-parser')
var session = require('express-session')
var morgan = require('morgan')
var path = require('path')

module.exports = (app) => {

  app.use(function(req, res, next) {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
    next();
  });

  app.use(compression())

  app.use(cookieParser())

  app.use(session({
    secret: 'hello_cello',
    name: '',
    proxy: true,
    resave: true,
    saveUninitialized: true
  }))

  app.use(express.static(path.join(__dirname, '..', 'build')))

  app.use(bodyParser.urlencoded({ extended: true }))

  app.use(bodyParser.json())

  app.use(methodOverride())

  morgan('combined', {
    skip: (req, res) => { return res.statusCode < 400 }
  })

  return app;
}