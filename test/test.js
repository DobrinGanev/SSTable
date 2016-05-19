
var assert = require("assert");
var SSTable = require('../index.js');


var fs = require('fs');

function runByOne(generatorFunction) {
    var generatorItr = generatorFunction(resume);
    function resume( err , value ) {
        generatorItr.next(  err || value  );
    }
    generatorItr.next()
}

let testData = [
	{ email : 'ryan@place.com' , age : '34' , name : 'Ryan' },
	{ email : 'chantelle@place.com' , age : '26' , name : 'Chantelle' },
	{ email : 'nikki@place.com' , age : '37' , name : 'Nikki' },
	{ email : 'jim@place.com' , age : '22' , name : 'Jim' },
	{ email : 'sarah@place.com' , age : '19' , name : 'Sarah' },
	{ email : 'anne@place.com' , age : '32' , name : 'Anne' },
	{ email : 'frank@place.com' , age : '56' , name : 'Frank' },
	{ email : 'thomas@place.com' , age : '24' , name : 'Thomas' },
	{ email : 'claire@place.com' , age : '60' , name : 'Claire' },
	{ email : 'sam@place.com' , age : '24' , name : 'Sam' }
]

new SSTable( "_test.sst" , { id : 'email' , json_aware : true } , ( err , sstable ) => {
	sstable.create( testData , ( value , err ) => {
		sstable.seek( 'jim@place.com' , ( err , value ) => {
			console.log( err , value );
			require('fs').unlink('_test.sst');
		} );
	});
	
	var x = sstable.all_iteraterable( );
	//console.log( x.next() );
	//console.log( x.next() );
	//console.log( x.next() );
	
} );




/*describe( "todb insertion and retrieval test" , ( ) => {
	it( "an insert should be the same as a retrieval" , ( done ) => {

	} );
} );*/
/*
new SSTable( "test.sst" , null , ( err , sstable ) => {
	sstable.seek( 'bbob' , ( err , data ) => {
		console.log(  data.toString() );
	});
	//sstable.seekRange( 'a' , 'bbob' , ( err , data ) => {
	//	console.log( data );
	//} );
} );*/
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
