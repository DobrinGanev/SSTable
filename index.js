

const max_doc_size = 8; //max doc (key value pair) size of 9.99 MB
const doc_size_pad = Array(max_doc_size).fill().reduce(prev => prev = (prev ||  "") + "0"); //the padding string for they bytes, 8 0s
const max_table_size = 10; //max table size is aprox 1 GB
const table_size_pad = Array(max_table_size).fill().reduce(prev => prev = (prev ||  "") + "0"); //the padding string for they bytes, 8 0s
const version = 1;
const start_content_seperator = String.fromCharCode(2); //start of text ascii
const start_heading_seperator = String.fromCharCode(1); //start of heading ascii
const record_seperator = String.fromCharCode(30); //record seperator ascii

const default_options = {
	id : 'id'
}

var fs = require('fs');

var stream = require('stream');

/**
 * Creates an immutable stored string table, well kind of - it's actually an array of Objects sorted by a key
 * Each Object MUST HAVE A KEY!
 * @class SSTable
 */
class SSTable {
	/**
	 * @async
	 * @param  {string} the file location of the SSTable, if it's an existing SSTable then it will be loaded 
	 * 					as immutable.
	 * @param  {object} an options object, the default is { id : 'id' } which specifies the PK
	 * @param  {Function( Error , SSTable )} calls when loading is completed.
	 * @return {SSTable}
	 */
	constructor( path , options , cb ) {
		
		this._options = Object.assign( { } , default_options , options || { } );
			
		this._fd; 						//the file discriptor for this table
		this._path = path;				//the path and filename for this table
		this._size = 0;					//the size of this table
		this._writable = false;			//is this table mutable
		this._version = 1;				//the version of this table
		this._contentsStart = 0; 		//the offset for the end of the main data / the start of the table of contents
		this._contents = { }; 			//the first offset of the first character of a key, called the table of contents
		this._key = this._options.id; 	//the primary key in an object, used for sorting
		this._events = { }; 			// a list of events that are being listened to on the SSTable

		//Load the table
		this._load( cb );
	}

	_load( cb ) {
		//Load the table
		if ( !fs.existsSync( this._path ) ){
			this._writable = true;
			try {
				this._fd = fs.openSync( this._path , 'w' ); 
			} catch( e ) {
				return cb( e );
			}
			this._writeHeaderPlaceHolderSync( );   
		    cb( null , this );
		
		} else {
		
			this._writable = false;
			this._size = fs.statSync( this._path ).size;
			try {
				this._fd = fs.openSync( this._path , 'r' );
			} catch( e ) {
				return cb( e );
			} 
			this._loadHeaderInfoSync( );
			
			//if this table is not empty then load the contents
			if( this._contentsStart > max_table_size + 2 ) {
				this._loadTableOfContents( ( err ) => {
					cb( err , this );
				} );
			} else {
				cb( null , this );
			}

		}
	}

	/**
	 * Takes an array of objects and sorts them by key before writing the SSTable
	 * @async
	 * @param  {[Object]}
	 * @param  {Function( Error )}
	 */
	writeFromArray( array_of_objects , cb ) {
		if( !this._writable ) return cb( new Error( "This SSTable is non mutable. Try creating a new table and merging this one in." ));

		//sorts based on key, right now the key HAS TO EXIST.
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
	

		var write_size = 0; //the size of the combined dataset, this is used to place the table of contents.

		
		this._write_from_sorted_array( array_of_objects , ( err ) => {
			
			//finish writing by placing table of contents at the bottom of the document
			let buffer = new Buffer( JSON.stringify( this._contents ) );
			 fs.write(this._fd, buffer , 0 , buffer.byteLength , this._size , ( err ) => {
				//and the position of the table of contents in the header
			 	let buffer = new Buffer( pad( table_size_pad , this._size ) );
			 	fs.write(this._fd, buffer , 0 , buffer.byteLength , 2, ( err ) => {
			 		this.writable = false;
			 		cb( err );
			 	});
			 	
			 });
			
		} );
		
	

	}


	/**
	 * @private
	 * @async
	 * @param  {[Object]} A sorted array of objects
	 * @param  {Function( Error ) } 
	 */
	_write_from_sorted_array( an_array_of_documents , cb ) {
		if( an_array_of_documents.length < 1 ) {
			cb( );
			return;
		}

	
			let data = an_array_of_documents.shift();
			

			let key = "";
			key = data[ this._key ];
			
			//create information for content file
			if( !this._contents.hasOwnProperty( key[ 0 ] ) ) {
				this._contents[ key[ 0 ] ] = this._size;
			}
			
		

			var buffer = this._create_record(  data );//new Buffer( bytes + kv_pair );	

			 fs.write(this._fd, buffer , 0 , buffer.byteLength , this._size , ( err ) => {

			 	this._fireEvents( "RecordAdded" , [ this._size , data ] );
			 	this._size += buffer.byteLength;
				
				this._write_from_sorted_array(  an_array_of_documents , cb );

				

			 });

	}
	/**
	 * Creates a record object for writing
	 * @private
	 * @param  {Object}
	 * @return {Buffer}
	 */
	_create_record( doc ) {
	
		var data = JSON.stringify( doc ) + record_seperator;
		var bytes = pad( doc_size_pad , Buffer.byteLength( data ));
		return new Buffer( bytes + data );
	}

	/**
	 * parses the table of contents portion of the SSTable into memory
	 * @private
	 * @async
	 * @param  {Function( err )}
	 */
	_loadTableOfContents( cb ){
		var length = this._size - this._contentsStart;
		var buf = new Buffer( length );
		
		fs.read( this._fd , buf , 0 , length , this._contentsStart , ( err , bytesRead , buffer ) => {
			this._contents = JSON.parse( buffer );

			cb( err  );
		});
	}

	/**
	 * Writes the header placeholder into the open file. The header consists of a version number
	 * and the offset of the table of contents
	 * @private
	 */
	_writeHeaderPlaceHolderSync( ) {
		//first 2 bytes is version information
		fs.writeSync(this._fd, pad( "00" , version ) , 0 , 2 );
		//next 10 bytes is the end of the data / access to the quick lookup information.
		fs.writeSync(this._fd, table_size_pad , 2 , max_table_size );
		this._size += max_table_size + 2;
		//The quick lookup information is a list of all the first characters in the SSTable and an offset of where to find them.
	}
	/**
	 * loads the header info into memory
	 * @private
	 */
	_loadHeaderInfoSync( ) {
		var versionBuffer = new Buffer( 2 );
		fs.readSync( this._fd , versionBuffer , 0 , 2 , 0 );
		console.log( `Loading table, version: ${versionBuffer.toString()}` );
		this._version = parseInt( versionBuffer );

		var offsetBuffer = new Buffer( max_table_size );
		fs.readSync( this._fd , offsetBuffer , 0 , max_table_size , 2 );
	
		this._contentsStart = parseInt( offsetBuffer );
		
	}

	/**
	 * Reads a record from a startign offset
	 * @private
	 * @async
	 * @param  {int} the offset of the record in the file
	 * @param  {Function( err , end_offset , buffer ) } the offset of the end of this record and a buffer of this record
	 */
	_readItem( start , cb ) {
		this._read( start , max_doc_size -1  , 
				( err , offset , buffer ) => { 
					this._read( offset , parseInt( buffer ) - 1 , //we -1 to take into account the record seperator character
						( err , offset , buffer ) => {

							cb( err , offset + 1 , buffer ); //we +1 to take into account the record seperator character
						} );
				} );
	}

	/**
	 * reads bytes from the file and outputs a buffer
	 * @private
	 * @async
	 * @param  {int} the start byte
	 * @param  {int} the end byte
	 * @param  {Function( err , endLength , buffer ) } the end position of this record and a buffer of its contents
	 * @return {[type]}
	 */
	_read( start , length , cb ) {
		var buf = new Buffer( length );
		fs.read( this._fd , buf , 0 , length , start , ( err , bytesRead , buffer ) => {
			cb( err , start  + length , buffer );
		});
		
	}

	/**
	 * Takes an SSTable and an append-log and merges them together in this document.
	 * @async
	 * @param  {SSTable}
	 * @param  {append-log}
	 * @param  {Function( Error )}
	 */
	mergeLog( sstable , log , cb ) {

		let temp_holder = { };  //to merge we hold everything in memory, this is just a quick hack to get things working
								// eventually we will have to do it with the live streams.
		if( !this._writable ) {
			cb( new Error( "This SSTable is non mutable. Try creating a new table and merging this one in." ));
		}

		let sstableStream = sstable.toStream( );
		sstableStream.on('data', ( doc ) => {
		    doc = JSON.parse( doc );
		    temp_holder[ doc[ this._key ] ] = doc;
		});

		sstableStream.on('end', () => {
		    let logStream = log.toStream( );
		    logStream.on('data', ( log ) => {
		    	if( log ) {
				    log = JSON.parse( log );
				   	if( log.verb == 'del' ) {
				   		delete temp_holder[ log.doc[ this._key ] ];
				   	} else {
				   		temp_holder[ log.doc[ this._key ] ] = log.doc
				   	}
			    }	
			});

			logStream.on('end', () => {
				let output = [ ];
				for( let key of Object.keys( temp_holder ) ) {
					output.push( temp_holder[ key ] );
				}

			    this.writeFromArray( output , ( err ) => {
			    	cb( err );
			    } );
			});
		});
	}

	/**
	 * @async
	 * Finds a record based on its key
	 * @param  {String} key
	 * @param  {Function( Error , doc )}
	 */
	seek( key , cb ) {
		this._seek( this._contents[ key[ 0 ] ] , key , cb );

	}
	/**
	 * Goes through from a start point and tries to find an object with a key
	 * @async
	 * @param  {int}
	 * @param  {string}
	 * @param  {Function( Error , Object)}
	 */
	_seek( offset , key , cb ) {
		this._readItem( offset , ( err , offset , data ) => {
			let doc = JSON.parse( data );
			if( doc[ this._key ] === key ) {
				return cb( null , doc );
			}
			this._seek( offset , key , cb  );
		} );
	}

	/**
	 * a public API for _readItem
	 * @async
	 */
	offset( offset , cb ) {
		this._readItem( offset , cb );
	}
	/**
	 * add an event listener
	 * @async
	 * @param  {string} event name
	 * @param  {Function} the call back when an event is fired
	 */
	on( name , cb ) {
		if( !this._events.hasOwnProperty( name ) ) {
			this._events[ name ] = [ ];
		}

		this._events[ name ].push( cb );
		return this._events[ name ].length;
	}

	/**
	 * fires event callback
	 * @param  {string} the name of the event
	 * @param  {[arguments to fire]}
	 */
	_fireEvents( name , args ) {
		if( this._events.hasOwnProperty( name ) ) {
			this._events[ name ].forEach( ( value ) => {
				value( ...args );
			});
		}
	}

	/**
	 * returns a read stream of this table
	 */
	toStream( ) {	
			var rs = stream.Readable();
			let this_offset = max_table_size + 2;
			rs._read = function () {

				if( this_offset >= this._contentsStart ) {
					return rs.push( null );
				}
				this.offset( this_offset , ( err , end_offset , data ) => {
					this_offset = end_offset;
					rs.push( data );
				});
			}.bind( this );
			return rs;
	}

	close( ) {
		fs.close( this._fd );
	}

}

module.exports = SSTable;

function pad(pad, str) {
    return (pad + str).slice(-pad.length);
}
