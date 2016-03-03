'use strict'

function IngestFast () {
  /**
   * Will prompt for the location of the FAST data
   *
   * @param  {function} cb - Nothing returned
   */
  this.ingest = require(`${__dirname}/lib/ingest`)(this)
}

module.exports = exports = new IngestFast()
