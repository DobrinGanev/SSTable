
var assert = require("assert");
var SSTable = require('../index.js');

new SSTable( "test.sst" , null , ( err , sstable ) => {
	sstable.seek( 'bbob' , ( err , data ) => {
		console.log(  data.toString() );
	});
	sstable.seekRange( 'a' , 'bbob' , ( err , data ) => {
		console.log( data );
	} );
} );
/*
sstable.create( {
	'bbob' : "TESTB",
	'a' : "TESTA",
	3 : "TESTCXZY"
} , ( err ) => {
	console.log( err );
});;*/
/*describe( "todb insertion and retrieval test" , ( ) => {
	it( "an insert should be the same as a retrieval" , ( done ) => {
		var db = new DB("./_test.db" , ( err , db ) => {
			console.log( "DB loaded" , err , db );
			db.put( "RA" , "CAT" , ( err ) => {
				
				 db.get( "RA" , ( err , value ) => {
					assert.equal( "CAT" , value );
					done();
				} );
			});

			db.put( "FOO" , "BAH" , ( err ) => {
				
				 db.del( "FOO" , ( err , value ) => {
					//assert.equal( "CAT" , value );
					//done();
				} );
			});
			//db.close();
		});
	})
		
});*/
