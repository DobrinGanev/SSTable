//todo rename key_list to index.

const max_doc_size = 8; //max doc (key value pair) size of 9.99 MB
const doc_size_pad = Array(max_doc_size).fill().reduce(prev => prev = (prev ||  "") + "0"); //the padding string for they bytes, 8 0s
const max_table_size = 10; //max table size is aprox 1 GB
const table_size_pad = Array(max_table_size).fill().reduce(prev => prev = (prev ||  "") + "0"); //the padding string for they bytes, 8 0s
const version = 1;
var fs = require('fs');

class SSTable {
	constructor( path , options , cb ) {
		this._options = options = options || {
			seperate_key_file : false
		}
		this._fd;
		this._path = path;
		this._size = 0;
		this._writable = false;
		this._version = 1;
		this._lookupOffset = 0; //the offset for the end of the main data / the start of the lookup data
		this._key_list_offset = { }; //the first offset of the first character of a key
		this._keys_offset = { };

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

	//create a sstable from a dictionary of k_v_pairs
	create( dictionary_of_k_v_pairs , cb ) {
		if( !this._writable ) {
			cb( new Error( "This SSTable is non mutable. Try creating a new table and merging this one in." ));
		}

		var write_size = 0;

		var keys = Object.keys( dictionary_of_k_v_pairs );
		keys.sort( );
		
		this._write_from_array( keys , dictionary_of_k_v_pairs , ( err ) => {
			
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

	_write_from_array( keys , data , cb ) {
		if( keys.length < 1 ) {
			cb( );
			return;
		}
			var key = keys.shift( );

			if( !this._key_list_offset.hasOwnProperty( key[ 0 ] ) ) {
				this._key_list_offset[ key[ 0 ] ] = this._size;
			}
			if( this._options.seperate_key_file ) {
				if( this._keys_offset.hasOwnProperty( key ) ) {
					this._keys_offset[ key ].push( this._size );
				} else {
					this._keys_offset[ key ] = [ this._size ];
				}
			}

			var kv_pair =  JSON.stringify( { k : key , v : JSON.stringify( data[ key ] ) } );

			var bytes = pad( doc_size_pad , Buffer.byteLength( kv_pair ));
			var buffer = new Buffer( bytes + kv_pair );	

			 fs.write(this._fd, buffer , 0 , buffer.byteLength , this._size , ( err ) => {

				this._size += buffer.byteLength;
				this._write_from_array( keys , data , cb );
			 });

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
					this._read( offset , parseInt( buffer ) ,
						( err , offset , buffer ) => {
							cb( err , offset , buffer );
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
			if( doc.k === key ) {
				return cb( null , data );
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

	offset( offset , cb ) {

	}

	offsetRange( offset_start , offset_end , cb ) {

	}

}

module.exports = SSTable;

function pad(pad, str) {
    return (pad + str).slice(-pad.length);
}
