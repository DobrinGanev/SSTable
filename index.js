//todo rename key_list to index.

const max_doc_size = 8; //max doc (key value pair) size of 9.99 MB
const doc_size_pad = Array(max_doc_size).fill().reduce(prev => prev = (prev ||  "") + "0"); //the padding string for they bytes, 8 0s
const max_table_size = 10; //max table size is aprox 1 GB
const table_size_pad = Array(max_table_size).fill().reduce(prev => prev = (prev ||  "") + "0"); //the padding string for they bytes, 8 0s
const version = 1;
const start_content_seperator = String.fromCharCode(2); //start of text ascii
const start_heading_seperator = String.fromCharCode(1); //start of heading ascii
const record_seperator = String.fromCharCode(30); //record seperator ascii

const default_options = {
	id : 'id',
	json_aware : false
}

var fs = require('fs');

class SSTable {
	constructor( path , options , cb ) {
		this._options = Object.assign( { } , default_options , options || { } );
			
		this._fd;
		this._path = path;
		this._size = 0;
		this._writable = false;
		this._version = 1;
		this._lookupOffset = 0; //the offset for the end of the main data / the start of the lookup data
		this._key_list_offset = { }; //the first offset of the first character of a key
		this._keys_offset = { };
		this._key = this._options.id;
		this._json_aware = this._options.json_aware;
		this._events = { }; // a list of events that are being listened to on the SSTable

		if (!fs.existsSync(this._path)){
			this._writable = true;
			this._fd = fs.openSync( this._path , 'w' ); 
			this._headerPlaceHolderSync( );
		    cb( null , this );
		} else {
			this._writable = false;
			this._size = fs.statSync( this._path ).size;

			this._fd = fs.openSync( this._path , 'r' ); 
			this._loadHeaderInfoSync( );
			this._loadIndex( ( err ) => {
				cb( err , this );
			} );

		}
	}


	//create a sstable from an array of objects
	create( array_of_objects , cb ) {
		if( !this._writable ) {
			cb( new Error( "This SSTable is non mutable. Try creating a new table and merging this one in." ));
			return;
		}
		if( this._json_aware) {
			array_of_objects.sort( ( a , b ) => {
				var a_key = a[ this._key ];
				var b_key = b[ this._key ];
				if( a_key < b_key ) {
					return -1;
				}
				if( a_key > b_key ) {
					return 1;
				}
				return 0; //must be equal, this shouldn't be though...
			} );
		} else {
			array_of_objects.sort();
		}

		var write_size = 0;

		
		this._write_from_sorted_array( array_of_objects , ( err ) => {
			
			//finish writing by placing the in memory quick lookup list at the bottom of the document
			let buffer = new Buffer( JSON.stringify( this._key_list_offset ) );
			 fs.write(this._fd, buffer , 0 , buffer.byteLength , this._size , ( err ) => {
				//and the position of this quick lookup list in the header
			 	let buffer = new Buffer( pad( table_size_pad , this._size ) );
			 	fs.write(this._fd, buffer , 0 , buffer.byteLength , 2, ( err ) => {
			 		this.writable = false;
			 		cb( err );
			 	});
			 	
			 });
			
		} );
		
	

	}

	_write_from_sorted_array( an_array_of_documents , cb ) {
		if( an_array_of_documents.length < 1 ) {
			cb( );
			return;
		}

	
			let data = an_array_of_documents.shift();
			

			let key = "";
			if( this._json_aware ) {
				key = data[ this._key ];
			} else {
				key = data;
			}
			
			
			//create information for content file
			if( !this._key_list_offset.hasOwnProperty( key[ 0 ] ) ) {
				this._key_list_offset[ key[ 0 ] ] = this._size;
			}
			

			//instead of below we should emit a key created event with the key name, the key offset , and the key size
			if( this._keys_offset.hasOwnProperty( key ) ) {
				this._keys_offset[ key ].push( this._size );
			} else {
				this._keys_offset[ key ] = [ this._size ];
			}
		

			var buffer = this._create_record(  data );//new Buffer( bytes + kv_pair );	

			 fs.write(this._fd, buffer , 0 , buffer.byteLength , this._size , ( err ) => {

				this._size += buffer.byteLength;
				this._write_from_sorted_array(  an_array_of_documents , cb );
			 });

	}
	/**
	* Returns a Buffer of a document
	*/
	_create_record( doc ) {
	
		var data = JSON.stringify( doc ) + record_seperator;
		var bytes = pad( doc_size_pad , Buffer.byteLength( data ));
		return new Buffer( bytes + data );
	}


	_loadIndex( cb ){
		var length = this._size - this._lookupOffset;
		var buf = new Buffer( length );
		
		fs.read( this._fd , buf , 0 , length , this._lookupOffset , ( err , bytesRead , buffer ) => {
			this._key_list_offset = JSON.parse( buffer );

			cb( err  );
		});
	}


	_headerPlaceHolderSync( ) {
		//first 2 bytes is version information
		fs.writeSync(this._fd, pad( "00" , version ) , 0 , 2 );
		//next 10 bytes is the end of the data / access to the quick lookup information.
		fs.writeSync(this._fd, table_size_pad , 2 , max_table_size );
		this._size += max_table_size + 2;
		//The quick lookup information is a list of all the first characters in the SSTable and an offset of where to find them.
	}

	_loadHeaderInfoSync( ) {
		var versionBuffer = new Buffer( 2 );
		fs.readSync( this._fd , versionBuffer , 0 , 2 , 0 );
		console.log( "`Loading table, version: ${versionBuffer.toString()}`" );
		this._version = parseInt( versionBuffer );

		var offsetBuffer = new Buffer( max_table_size );
		fs.readSync( this._fd , offsetBuffer , 0 , max_table_size , 2 );
	
		this._lookupOffset = parseInt( offsetBuffer );
		
	}

	_readItem( start , cb ) {
		this._read( start , max_doc_size -1  , 
				( err , offset , buffer ) => { 
					this._read( offset , parseInt( buffer ) - 1 , //we -1 to take into account the record seperator character
						( err , offset , buffer ) => {

							cb( err , offset + 1 , buffer ); //we +1 to take into account the record seperator character
						} );
				} );
	}

	_read( start , length , cb ) {
		
		var buf = new Buffer( length );
		
		fs.read( this._fd , buf , 0 , length , start , ( err , bytesRead , buffer ) => {
			cb( err , start  + length , buffer );
		});
		
	}


	merge( first , second , cb ) {
		if( !this._writable ) {
			cb( new Error( "This SSTable is non mutable. Try creating a new table and merging this one in." ));
		}
	}

	seek( key , cb ) {
		this._seek( this._key_list_offset[ key[ 0 ] ] , key , cb );

	}

	_seek( offset , key , cb ) {
		this._readItem( offset , ( err , offset , data ) => {
			let doc = JSON.parse( data );
			if( doc[ this._key ] === key ) {
				return cb( null , doc );
			}
			this._seek( offset , key , cb  );
		} );
	}

	seekRange( key_start , key_end , cb ) {
		this._seekRange( this._key_list_offset[ key_start[ 0 ] ] , key_start ,key_end , cb );
	}
	_seekRange( offset , key_start , key_end , cb , resultArray ) {
		resultArray = resultArray || [ ];
		this._readItem( offset , ( err , offset , data ) => {
			let doc = JSON.parse( data );
			if( doc.k >= key_start ) {
				resultArray.push( data );
				this._seekRange( offset , key_start , key_end , cb , resultArray  );
			} else if( doc.k <= key_end ) {
				resultArray.push( data );
				return cb( null , resultArray );
			}
			
		} );
	}

	log( message ) {
		console.log( message );
	}

	offset( offset , cb ) {
		this._readItem( offset , cb );
	}

	offsetRange( offset_start , offset_end , cb ) {

	}

	all_iteraterable( ) {
		var iterator = this._all_iteraterable( next );
		function next( ) {
			console.log("NEXT");
			iterator.next() 
		}
		iterator.next();


		return iterator;
	}

	* _all_iteraterable( done ) {
		
		let this_offset = max_table_size + 2; //take into account the header
		var yielded_data = "rumple";
		console.log("S");
		//console.log(caller.offset);
		yield this.offset( this_offset , ( err , end_offset , data ) => { 
			this_offset = end_offset;
			yielded_data = data;
			done();
		});

		console.log("X ", yielded_data );
		
		//console.log( yielded_data );
		/*caller.offset( this_offset , ( err , end_offset , data ) => {
			caller.log( message );
			this_offset = end_offset;
			yielded_data = data;
			console.log( data );
			console.log("HERE");
			//return data;
			//iterator.next();
		});*/
			yield yielded_data;

		//	}
	}

}

module.exports = SSTable;

function pad(pad, str) {
    return (pad + str).slice(-pad.length);
}
