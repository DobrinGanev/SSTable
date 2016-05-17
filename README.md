# SSTable
Sorted String Tables

As used by todb, it's an immutable SStable with an index for quick lookup.

Can only be created with a dictionary.

Under heavy development, do not use.

Example:

```javascript
new SSTable( "test.sst" , null , ( err , sstable ) => {
	sstable.create( {
		1 : 'one',
		'two' : 'two',
		'3' : { 'value' = '3' }
	} , ( err ) => {
		sstable.seek( 1 , ( err , data ) => {
			console.log( data );
		} );
	});

} );
```
