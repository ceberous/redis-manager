# redis-manager-utils

### Some main.js
```
const RMU = require( "redis-manager-utils" );
( async ()=> {
	console.log( "Starting" );
	var my_con_1 = new RMU( 1 );
	await my_con_1.init();
	module.exports.redisConProxy = my_con_1;
	console.log( await my_con_1.exists( "TESTING_KEY_1" ) );
	await require( "./test.js" ).tryTest( "TESTING_NEW_KEY_2" , "NEW_VALUE_2" );
	console.log( "Ending" );
	my_con_1.quit();
})();
```

### test.js
```
function Try_Test( wKey , wVal ) {
	return new Promise( async function( resolve , reject ) {
		try {
			await require( "./main.js" ).redisConProxy.keySet( wKey , wVal );
			resolve();
		}
		catch( error ) { console.log( error ); reject( error ); }
	});
}
module.exports.tryTest = Try_Test;
```
