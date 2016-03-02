var should = require('should')
var fastParse = require('../lib/fast_parse.js')


describe('fast parse', function () {


	it('empty triple text', function (done) {
		fastParse.tripleToObj("Not a triple text", (error, results)=>{
			results.should.equal("")
			done()
		})
	})

	it('test triple', function (done) {
		fastParse.tripleToObj('<http://id.worldcat.org/fast/799409> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Intangible> .', (error, results)=>{
			results.id.should.equal(799409)
			done()
		})
	})



	it('parse fast ID', function (done) {
		fastParse.tripleToObj('<http://id.worldcat.org/fast/799409> <http://purl.org/dc/terms/identifier> "799409" .', (error, results)=>{
			results.id.should.equal(799409)
			done()
		})
	})

	it('parse sameAs VIAF', function (done) {
		fastParse.tripleToObj('<http://id.worldcat.org/fast/1408092> <http://schema.org/sameAs> <http://viaf.org/viaf/143214773> .', (error, results)=>{
			results.sameAsViaf.should.equal('http://viaf.org/viaf/143214773')
			done()
		})
	})
	it('parse sameAs LC', function (done) {
		fastParse.tripleToObj('<http://id.worldcat.org/fast/1794112> <http://schema.org/sameAs> <http://id.loc.gov/authorities/names/no2008059410> .', (error, results)=>{
			results.sameAsLc.should.equal('http://id.loc.gov/authorities/names/no2008059410')
			done()
		})
	})
	it('parse sameAs LC LCSH', function (done) {
		fastParse.tripleToObj('<http://id.worldcat.org/fast/825323> <http://schema.org/sameAs> <http://id.loc.gov/authorities/subjects/sh85010894> .', (error, results)=>{
			results.sameAsLc.should.equal('http://id.loc.gov/authorities/subjects/sh85010894')
			done()
		})
	})

	it('parse prefLabel', function (done) {
		fastParse.tripleToObj('<http://id.worldcat.org/fast/1407208> <http://www.w3.org/2004/02/skos/core#prefLabel> "Regional Farm Policy Conference" .', (error, results)=>{
			results.prefLabel.should.equal('Regional Farm Policy Conference')
			results.normalized.should.equal('regional farm policy conference')
			done()
		})
	})

	it('parse altLabel', function (done) {
		fastParse.tripleToObj('<http://id.worldcat.org/fast/1410700> <http://www.w3.org/2004/02/skos/core#altLabel> "Fiesta San Antonio (San Antonio, Tex.)" .', (error, results)=>{
			results.altLabel.should.equal('Fiesta San Antonio (San Antonio, Tex.)')
			results.normalized.should.equal('fiesta san antonio san antonio tex')
			done()
		})
	})

	it('parse rdf label', function (done) {
		fastParse.tripleToObj('<http://id.worldcat.org/fast/1410700> <http://www.w3.org/2000/01/rdf-schema#label> "Carlsbad International Chess Tournament" .', (error, results)=>{
			results.label.should.equal('Carlsbad International Chess Tournament')
			results.normalized.should.equal('carlsbad international chess tournament')
			done()
		})
	})
	it('parse sameAs Label', function (done) {
		fastParse.tripleToObj('<http://id.loc.gov/authorities/names/fst01715782> <http://www.w3.org/2000/01/rdf-schema#label> "Aviation insurance--War risks--Law and legislation" .', (error, results)=>{
			results.label.should.equal('Aviation insurance--War risks--Law and legislation')
			results.subject.should.equal('http://id.loc.gov/authorities/names/fst01715782')
			results.type.should.equal('sameAsLabel')
			done()
		})
	})


})