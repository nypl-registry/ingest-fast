'use strict'

var utils = require('nypl-registry-utils-normalize')

var N3 = require('n3')
var parser = N3.Parser()
var N3Util = N3.Util

var exports = module.exports = {}

/**
 * Takes the triple object parsed by N3 and figures out what we want out of it
 *
 * @param  {object} triple - the n3 parsed triple.
 * @return {object} THe new object with specific predicates we are interested in
 */
exports.filterTripleObj = function (triple) {
  if (triple.subject.search('/fast/') > -1) {
    var obj = {type: 'fast'}

    if (triple.subject.search('/fast/NaN') > -1 || triple.object.search('/fast/NaN') > -1) return ''

    obj.id = parseInt(triple.subject.split('/fast/')[1])

    if (triple.predicate === 'http://schema.org/sameAs') {
      if (triple.object.search('id.loc.gov') > -1) obj.sameAsLc = triple.object
      if (triple.object.search('viaf.org') > -1) obj.sameAsViaf = triple.object
    }

    if (triple.predicate === 'http://www.w3.org/2004/02/skos/core#prefLabel' || triple.predicate === 'http://www.w3.org/2004/02/skos/core#altLabel' || triple.predicate === 'http://www.w3.org/2000/01/rdf-schema#label') {
      var o = N3Util.getLiteralValue(triple.object)

      if (o.length >= 2) {
        if (triple.predicate === 'http://www.w3.org/2004/02/skos/core#prefLabel') obj.prefLabel = o
        if (triple.predicate === 'http://www.w3.org/2004/02/skos/core#altLabel') obj.altLabel = o
        if (triple.predicate === 'http://www.w3.org/2000/01/rdf-schema#label') obj.label = o
        var normalO = utils.singularize(utils.normalizeAndDiacritics(o))
        obj.normalized = normalO
      }
    }

    return obj
  } else {
    if (triple.predicate === 'http://www.w3.org/2000/01/rdf-schema#label') {
      var sameAs = {type: 'sameAsLabel'}
      sameAs.label = N3Util.getLiteralValue(triple.object)
      sameAs.subject = triple.subject
      sameAs.normalized = utils.singularize(utils.normalizeAndDiacritics(sameAs.label))

      return sameAs
    } else {
      return ''
    }
  }
}

/**
 * Takes a NT triple and parses it into an object based on what we need to populate the FAST lookup
 *
 * @param  {string} tripleText - The triple in NT notation.
 * @param  {function} cb - "fast" type object returned or "sameAsLabel" with the specific object as a property
 */
exports.tripleToObj = function (tripleText, cb) {
  parser.parse(tripleText, function (error, triple, prefixes) {
    if (triple) {
      cb(error, exports.filterTripleObj(triple))
    } else {
      cb(error, '')
    }

    if (error) {
      console.log(error)
    }
    return true
  })
}
