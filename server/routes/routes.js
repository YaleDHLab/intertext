var path = require('path')
var api = require('./api')

module.exports = (app) => {

  app = api(app);

  app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, '..', '..', 'build', 'index.html'))
  })

  return app;
}