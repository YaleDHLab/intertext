const config = require('../config');
var queryParser = {};

/***
* Helpers that escape non-unicode characters, which are not
* allowed in urls but which could appear in urls within this
* application.
*
* usage unicodeEscape(nonUnicodeUrl);
*
* @author: Dominic
*   originally posted in SO 7499473
***/

queryParser.unicodeEscape = function(string) {
  return string.split('').map(function (char) {
    var charCode = char.charCodeAt(0);
    return charCode > 127 ? unicodeCharEscape(charCode) : char;
  }).join('');
}

queryParser.padWithLeadingZeros = function(string) {
  return new Array(5 - string.length).join('0') + string;
}

queryParser.unicodeCharEscape = function(charCode) {
  return '\\u' + padWithLeadingZeros(charCode.toString(16));
}

/***
* Helper that escapes regex characters in order to make them
* searchable by server.js.
*
* @author: Mathias Bynens
*   originally posted in SO 3115150
***/

queryParser.regexEscape = function(text) {
  return text.replace(/[-[\]{}()*+?.,\\^$|#\s]/g, '\\$&');
};

module.exports = queryParser;