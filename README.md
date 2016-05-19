# SSTable

This used to be a library for creating Sorted String Tables, as the needs of [todb](https://github.com/disordinary/todb) have changed so has the nature of this library. It's still kind of a SSTable but it's evolved.

An SSTable traditionally stores strings in sorted order something like this: `key,value|key,value|key,value...`

This library stores serialised JSON objects sorted by their key so:

`{ key : 'a' , value : 'foo' },{ key : 'b' , value : 'bah'}`, etc.

It's actually a little more complicated than that, it goes:
 0029{key : 'a' , value : 'foo' }0x1e0029{key : 'a' , value : 'foo' }0x1e

 Where 0029 is the byte length of the proceeding string and 0x1e is the row deliminator ascii record. Aditionally the first 12 bytes are reserved, the first two annotate the version of the SSTable, the next 10 the position in the table where the content stops. After all the records is a serialized JSON lookup table that I call the contents, this stores the byte offset for every section of keys, currently that is done based on the first letter but eventually it will be based on every x% of the table, or x rows, or x bytes, etc.

 So if I want to look for a record starting with the letter x SSTable will first look at the contents and see where the byte offset for all the `x` records are and look from there rather than the start of the file.

Example:

```javascript

new SSTable( "test.sst" , { id : 'email'  } , ( err , sstable ) => {
	sstable.create( [ { email : 'nikki@place.com' , age : '37' , name : 'Nikki' } ] , ( err ) => {
		sstable.seek( 'nikki@place.com' , ( err , value ) => {
		console.log(  value );
		} );
	} );
} );
```
