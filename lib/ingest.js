'use strict'

// This script might take up >1GB (per process) of ram. Might not run on the VPS

/*
	This is a cluster script that loads data from a few oclc FAST files and inserts it into the registry-ingest database
	The problem is theses files are NT triple files, so if you are streaming them you need to build up the complete data object in memory
	The triples are not nessisarly in order so you have to wait until the end of the file to know that they are complte
	the master part of the script asks for the path and spawns a worker for each file
	the worker part of the script streams each file and build the in memory object for each term
	If the file is a Agent file (FAST has pers,corp,event names) then it is not inserted into the FAST collection it checks if the fast id is in the coropsonding VIAF record yet and if not adds it (updateViafRecord)
	There are some problems with the FAST datafiles, this script works around them. If there is an error in the bulk insert (insert function) it will try to figure out which term was not added (due to dupe key error) and then update the existing record in the fast collection (updateToBestFast)
*/

module.exports = function () {
  return function (callback) {
    var db = require('nypl-registry-utils-database')
    var cluster = require('cluster')
    var prompt = require('prompt')
    var clc = require('cli-color')
    var fs = require('fs')

    var total = 0
    var updatedViaf = 0

    if (cluster.isMaster) {
      // first find out where the files are, default to the data directory of the execution
      var promptSchema = {
        properties: {
          path: {
            message: 'Path to FAST data',
            required: true,
            default: process.cwd() + '/data/'
          }
        }
      }

      prompt.start()
      prompt.get(promptSchema, function (err, result) {
        if (err) console.log(err)
        // make sure all the files are there
        try {
          fs.statSync(`${result.path}FASTChronological.nt`)
          fs.statSync(`${result.path}FASTCorporate.nt`)
          fs.statSync(`${result.path}FASTEvent.nt`)
          fs.statSync(`${result.path}FASTGeographic.nt`)
          fs.statSync(`${result.path}FASTPersonal.nt`)
          // No Title - we are not doing anything with title authorities right now
          // fs.statSync(`${result.path}FASTTitle.nt`)
          fs.statSync(`${result.path}FASTTopical.nt`)
        } catch (e) {
          console.log(e)
          throw new Error('Not all the FAST NT files were found, make sure that is the right directory.')
        }

        // ,`${result.path}FASTTitle.nt`
        var files = [`${result.path}FASTChronological.nt`, `${result.path}FASTCorporate.nt`, `${result.path}FASTEvent.nt`, `${result.path}FASTGeographic.nt`, `${result.path}FASTPersonal.nt`, `${result.path}FASTTopical.nt`]

        // to test:
        // var files = [`${result.path}FASTGeographic.nt`]

        console.log(clc.whiteBright.bgRedBright('----- About to Drop the FAST collection in 5 seconds ----- ctrl-c now to abort'))
        setTimeout(function () {
          db.prepareFast(function () {
            db.databaseClose()

            JSON.parse(JSON.stringify(files)).forEach((filepath) => {
              var worker = cluster.fork()
              // the worker will send a request message to us for two reasons
              worker.on('message', function (msg) {
                // asking for the file name to use, send them the filename
                if (msg.request) {
                  worker.send({ work: files.shift() })
                }

                if (msg.updatedViaf) updatedViaf++
                if (msg.report) total = total + msg.report

                // reporting back how many clusters it has inserted
                if (msg.report || msg.updatedViaf) {
                  process.stdout.cursorTo(0)
                  process.stdout.write('Total Inserted: ' + clc.black.bgGreenBright(total) + ' updatedViaf: ' + clc.black.bgGreenBright(updatedViaf))
                }
              })
            })
          })
        }, 5000)
      })
      // called when one of the workers quit
      cluster.on('exit', (worker, code, signal) => {
        if (Object.keys(cluster.workers).length === 0) {
          if (callback) callback()
        }
      })
    } else {
      // THE WORKER
      var fastParse = require(`${__dirname}/fast_parse`)
      var _ = require('highland')
      var N3 = require('n3')

      var allFast = {}
      var allSameAs = {}

      // Updates the VIAF records with the FAST ID
      var updateViafRecord = function (fast, cb) {
        // /console.log(fast)
        db.returnCollectionRegistry('viaf', function (err, viafCollection) {
          if (err) console.log(err)
          viafCollection.find({$or: [{viaf: fast.otherId}, {lcId: fast.otherId}]}, {fast: 1}).toArray((err, results) => {
            if (err) console.log(err)
            if (results.length > 0) {
              var viaf = results[0]
              var existingFast = (viaf.fast) ? viaf.fast : []

              if (existingFast.indexOf(fast.id) === -1) {
                existingFast.push(fast.id)
                viafCollection.update({ viaf: viaf._id }, { $set: {fast: existingFast} }, function (err, res) {
                  if (err) console.log(err)
                  process.send({ updatedViaf: true })
                  cb()
                })
                return true
              }
            }
            process.send({ updatedViaf: true })
            cb()
          })
        })
      }

      // This helper function is used to do a diff of what is in the db vs wanting to add a better record
      // sometimes a stub record will get added and there is better info in another file (type) to compare each field and make sure the best record is in there.
      var updateToBestFast = function (fast, cb) {
        db.returnCollectionRegistry('fast', function (err, fastCollection) {
          if (err) console.log(err)
          fastCollection.find({ _id: fast._id }).toArray((err, results) => {
            if (err) console.log(err)
            if (results.length > 0) {
              results = results[0]
              results.type = (JSON.stringify(fast).length > JSON.stringify(results).length) ? fast.type : results.type
              // check to make sure that all values are in each of the fields
              if (!results.prefLabel && fast.prefLabel) results.prefLabel = fast.prefLabel
              fast.altLabel.forEach((l) => {
                if (results.altLabel.indexOf(l) === -1) results.altLabel.push(l)
              })
              fast.sameAsLc.forEach((l) => {
                if (results.sameAsLc.indexOf(l) === -1) results.sameAsLc.push(l)
              })
              fast.sameAsViaf.forEach((l) => {
                if (results.sameAsViaf.indexOf(l) === -1) results.sameAsViaf.push(l)
              })
              fast.normalized.forEach((l) => {
                if (results.normalized.indexOf(l) === -1) results.normalized.push(l)
              })
              fastCollection.update({ _id: fast._id }, { $set: results }, function (err, res) {
                if (err) console.log(err)
                cb()
              })
              return true
            }
            cb()
          })
        })
      }

      // the bulk insert function, it gets a unordered bulk operation from the db
      var insert = function (fast, callback) {
        // ask for a new bulk operation
        db.newRegistryIngestBulkOp('fast', (bulk) => {
          // insert all the operations
          fast.forEach((a) => bulk.insert(a))
          bulk.execute(function (err, result) {
            if (err) {
              if (err.toString().search('duplicate key error') === -1) console.log(err)
              // on an error go though and update all of them
              _(fast)
                .map(_.curry(updateToBestFast))
                .nfcall([])
                .parallel(20)
                .done(function () {
                  callback()
                })
            } else {
              callback()
            }
            // tell how many we just updated
            process.send({ report: fast.length })
          })
        })
      }

      process.on('message', (msg) => {
        var streamParser = new N3.StreamParser()
        var inputStream = fs.createReadStream(msg.work)
        inputStream.pipe(streamParser)

        // Set the TYPE
        var type
        if (msg.work.search('FASTCorporate') > -1) type = 'Corporate'
        if (msg.work.search('FASTEvent') > -1) type = 'Event'
        if (msg.work.search('FASTPersonal') > -1) type = 'Personal'
        if (msg.work.search('FASTChronological') > -1) type = 'Chronological'
        if (msg.work.search('FASTGeographic') > -1) type = 'Geographic'
        if (msg.work.search('FASTTitle') > -1) type = 'Title'
        if (msg.work.search('FASTTopical') > -1) type = 'Topical'

        if (msg.work.search('FASTCorporate') > -1 || msg.work.search('FASTEvent') > -1 || msg.work.search('FASTPersonal') > -1) {
          console.log(cluster.worker.id, 'AGENT', msg)
          _(streamParser)
            .compact()
            .map(fastParse.filterTripleObj)
            .map((obj) => {
              // grab the LC or VIAF id to find it in the VIAF collection
              if (obj.sameAsLc) {
                obj.otherId = obj.sameAsLc.split('/')[obj.sameAsLc.split('/').length - 1]
                return obj
              }
              if (obj.sameAsViaf) {
                obj.otherId = obj.sameAsViaf.split('/')[obj.sameAsViaf.split('/').length - 1]
                return obj
              }
              return ''
            })
            .compact()
            .map(_.curry(updateViafRecord))
            .nfcall([])
            .parallel(20)
            .done(function () {
              console.log('Worker: ', cluster.worker.id, ' Done with', msg.work)
              process.exit(0)
            })
        } else {
          _(streamParser)
            .compact()
            .map(fastParse.filterTripleObj)
            .map((obj) => {
              // we are building a big object because we are dealing with n triples
              if (obj.type === 'fast') {
                if (!allFast[obj.id]) allFast[obj.id] = { fast: obj.id, prefLabel: false, altLabel: [], sameAsLc: [], sameAsViaf: [], normalized: [] }
                if (obj.prefLabel) allFast[obj.id].prefLabel = obj.prefLabel
                if (obj.label && !allFast[obj.id].prefLabel) allFast[obj.id].prefLabel = obj.label
                if (obj.altLabel) allFast[obj.id].altLabel.push(obj.altLabel)
                if (obj.sameAsLc) {
                  allFast[obj.id].sameAsLc.push(obj.sameAsLc)
                  allFast[obj.id].sameAsLc.push(obj.sameAsLc.split('/')[obj.sameAsLc.split('/').length - 1])
                }
                if (obj.sameAsViaf) {
                  allFast[obj.id].sameAsViaf.push(obj.sameAsViaf)
                  allFast[obj.id].sameAsViaf.push(obj.sameAsViaf.split('/')[obj.sameAsViaf.split('/').length - 1])
                }
                if (obj.normalized && allFast[obj.id].normalized.indexOf(obj.normalized) === -1) allFast[obj.id].normalized.push(obj.normalized)
              } else {
                if (!allSameAs[obj.subject]) allSameAs[obj.subject] = []
                allSameAs[obj.subject].push(obj)
              }

              return obj
            })
            .done(function () {
              var allInsert = []
              console.log('Building SameAs', msg.work)
              for (var x in allFast) {
                // there are sameAs labels in the NT file, we want to add those as ALT terms to our FAST lookup
                allFast[x].sameAsViaf.forEach((v) => {
                  if (allSameAs[v]) {
                    allSameAs[v].forEach((sameAs) => {
                      if (sameAs.label && allFast[x].altLabel.indexOf(sameAs.label) === -1) {
                        allFast[x].altLabel.push(sameAs.label)
                        allFast[x].normalized.push(sameAs.normalized)
                      }
                    })
                  }
                })
                allFast[x].sameAsLc.forEach((v) => {
                  if (allSameAs[v]) {
                    allSameAs[v].forEach((sameAs) => {
                      if (sameAs.label && allFast[x].altLabel.indexOf(sameAs.label) === -1) {
                        allFast[x].altLabel.push(sameAs.label)
                        allFast[x].normalized.push(sameAs.normalized)
                      }
                    })
                  }
                })

                allFast[x]._id = allFast[x].fast
                allFast[x].type = type

                allInsert.push(JSON.parse(JSON.stringify(allFast[x])))
                delete allFast[x]
              }
              // batch insert the FAST records
              console.log('Inserting', msg.work)
              _(allInsert)
                .batch(100)
                .map(_.curry(insert))
                .nfcall([])
                .series()
                .done(function () {
                  console.log('Worker: ', cluster.worker.id, ' Done with', msg.work)
                  process.exit(0)
                })
            })
        }
      })

      // ask for the file name
      process.send({ request: true })
    }
  }
}
