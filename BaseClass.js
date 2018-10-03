const REDIS = require("redis");
const { map } = require( "p-iteration" );

class RedisUtilsBase {

	constructor( databaseNumber , host , port , constants ) {
		if ( typeof databaseNumber === "object" ) {
			this.databaseNumber = databaseNumber.databaseNumber || 0;
			this.host = databaseNumber.host || "localhost";
			this.port = databaseNumber.port || 6379;
			this.c = databaseNumber.constants || {};
		}
		else {
			this.databaseNumber = databaseNumber || 0;
			this.host = host || "localhost";
			this.port = port || 6379;
			this.c = constants || {};
		}
		
		this.redis = undefined;			
	}

	async init() {
		this.redis = await REDIS.createClient( { 
			host: this.host ,
			port: this.port ,
			db: this.databaseNumber ,
			retry_strategy: function ( options ) {
				if ( options.error && options.error.code === "ECONNREFUSED" ) {
					// End reconnecting on a specific error and flush all commands with
					// a individual error
					return new Error( "The server refused the connection" );
				}
				if ( options.total_retry_time > 1000 * 60 * 60 ) {
					// End reconnecting after a specific timeout and flush all commands
					// with a individual error
					return new Error( "Retry time exhausted" );
				}
				if ( options.attempt > 20 ) {
					// End reconnecting with built in error
					return undefined;
				}
				// reconnect after
				return Math.min( options.attempt * 100 , 3000 );
			}
		} );		
	}

	quit() {
		this.redis.quit();
	}

	// Misc
	selectDatabase( wNumber ) {
		if ( !wNumber ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.select( wNumber , function( err , values ) { that.databaseNumber = wNumber; resolve( values ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});
	}

	exists( wKey ) {
		if ( !wKey ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try {
				that.redis.exists( wKey , function( err , answer ) {
					let wFinal = false;
					if ( answer === 1 || answer === "1" ) { wFinal = true; }
					resolve( wFinal );
				});
			}
			catch( error ) { console.log( error ); reject( error ); }
		});		
	}

	// Keys
	keyGet( wKey ) {
		if ( !wKey ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.get( wKey , function( err , key ) { resolve( key ); }); }
			catch( error ) { console.log( error ); resolve( "null" ); }
		});
	}

	keysGetFromPattern( wPattern ) {
		if ( !wPattern ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.keys( wPattern , function( err , keys ) { resolve( keys ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});
	}

	keyGetDeJSON( wKey ) {
		if ( !wKey ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.get( wKey , function( err , key ) {
				try { key = JSON.parse( key ); }
				catch( err ) {}
				resolve( key ); });
			}
			catch( error ) { console.log( error ); resolve( "null" ); }
		});
	}

	keyDel( wKey ) {
		if ( !wKey ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.del( wKey , function( err , keys ) { resolve( keys ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});
	}

	keySet( wKey , wVal ) {
		if ( !wKey ) { return undefined; }
		if ( !wVal ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.set( wKey , wVal , function( err , values ) { resolve( values ); }); }
			catch( error ) { console.log( error ); resolve( "error" ); }
		});
	}

	keySetIfNotExists( wKey , wVal ) {
		if ( !wKey ) { return undefined; }
		if ( !wVal ) { return undefined; }
		let that = this;
		return new Promise( async function( resolve , reject ) {
			if ( await that.exists( wKey ) === false ) {
				try { that.redis.set( wKey , wVal , function( err , values ) { resolve( values ); }); }
				catch( error ) { console.log( error ); resolve( "error" ); }
			}
			else { resolve(); }
		});	
	}

	keySetMulti( wArgs ) {
		if ( !wArgs ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.multi( wArgs ).exec( function( err , results ) { resolve( results ); }); }
			catch( error ) { console.log( error ); resolve( "error" ); }
		});	
	}

	keyGetMulti( ...args ) {
		if ( !args ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.mget( ...args , function( err , values ) { resolve( values ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});	
	}	

	increment( wKey ) {
		if ( !wKey ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.incr( wKey , function( err , values ) { resolve( values ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});
	}

	decrement( wKey ) {
		if ( !wKey ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.decr( wKey , function( err , values ) { resolve( values ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});	
	}

	// Lists
	listGetFull( wKey ) {
		if ( !wKey ) { return undefined; }
		let that = this;	
		return new Promise( function( resolve , reject ) {
			try { that.redis.lrange( wKey , 0 , -1 , function( err , values ) { resolve( values ); }); }
			catch( error ) { console.log( error ); resolve( "list probably doesn't even exist" ); }
		});
	}

	listGetLength( wKey ) {
		if ( !wKey ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.llen( wKey , function( err , key ) { resolve( key ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});
	}

	listTrim( wKey , wStart , wEnd ) {
		if ( !wKey ) { return undefined; }
		if ( !wStart ) { return undefined; }
		if ( !wEnd ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.ltrim( wKey , wStart , wEnd , function( err , key ) { resolve( key ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});
	}

	listGetByIndex( wKey , wIndex ) {
		if ( !wKey ) { return undefined; }
		if ( !wIndex ) { return undefined; }
		let that = this;		
		return new Promise( function( resolve , reject ) {
			try { that.redis.lindex( wKey , wIndex , function( err , key ) { resolve( key ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});	
	}

	listSetFromArray( wKey , wArray ) {
		if ( !wKey ) { return undefined; }
		if ( !wArray ) { return undefined; }
		let that = this;		
		return new Promise( function( resolve , reject ) {
			try { that.redis.rpush.apply( that.redis , [ wKey ].concat( wArray ).concat( function( err , keys ){ resolve( keys ); })); }
			catch( error ) { console.log( error ); reject( error ); }
		});
	}

	listSetFromArrayBeginning( wKey , wArray ) {
		if ( !wKey ) { return undefined; }
		if ( !wArray ) { return undefined; }
		let that = this;		
		return new Promise( function( resolve , reject ) {
			try { that.redis.lpush.apply( that.redis , [ wKey ].concat( wArray ).concat( function( err , keys ){ resolve( keys ); })); }
			catch( error ) { console.log( error ); reject( error ); }
		});
	}

	listRPOP( wKey ) {
		if ( !wKey ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.lpop( wKey , function( err , values ) { resolve( values ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});
	}

	listRPUSH( wKey , wValue ) {
		if ( !wKey ) { return undefined; }
		if ( !wValue ) { return undefined; }
		let that = this;		
		return new Promise( function( resolve , reject ) {
			try { that.redis.rpush( wKey , wValue , function( err , values ) { resolve( values ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});
	}

	// Sets
	setGetFull( wKey ) {
		if ( !wKey ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.smembers( wKey , function( err , values ) { resolve( values ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});
	}

	setLength( wKey ) {
		if ( !wKey ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.scard( wKey , function( err , values ) { resolve( values ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});	
	}

	setAdd( wKey , wValue ) {
		if ( !wKey ) { return undefined; }
		if ( !wValue ) { return undefined; }
		let that = this;		
		return new Promise( function( resolve , reject ) {
			if ( !wValue ) { resolve(); return; }
			if ( wValue === "" || wValue === "undefined" ) { resolve(); return; }
			try { that.redis.sadd( wKey , wValue , function( err , values ) { resolve( values ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});		
	}

	setRemove( wKey , wValue ) {
		if ( !wKey ) { return undefined; }
		if ( !wValue ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.srem( wKey , wValue , function( err , values ) { resolve( values ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});	
	}

	setRemoveMatching( wSetKey , wMatchKey ) {
		if ( !wSetKey ) { return undefined; }
		if ( !wMatchKey ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.srem( wSetKey , wMatchKey , function( err , key ) { resolve( key ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});		
	}

	setGetRandomMembers( wKey , wNumber ) {
		if ( !wKey ) { return undefined; }
		if ( !wNumber ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.srandmember( wKey , wNumber , function( err , values ) { resolve( values ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});		
	}

	setPopRandomMembers( wKey , wNumber ) {
		if ( !wKey ) { return undefined; }
		if ( !wNumber ) { return undefined; }
		let that = this;
		wNumber = wNumber || 1;
		return new Promise( function( resolve , reject ) {
			try { that.redis.spop( wKey , wNumber , function( err , values ) { resolve( values ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});
	}

	setSetFromArray( wKey , wArray ) {
		if ( !wKey ) { return undefined; }
		if ( !wArray ) { return undefined; }
		let that = this;		
		return new Promise( function( resolve , reject ) {
			try { that.redis.sadd.apply( that.redis , [ wKey ].concat( wArray ).concat( function( err , keys ){ resolve( keys ); })); }
			catch( error ) { console.log( error ); reject( error ); }
		});
	}

	setSetDifferenceStore( wStoreKey , wSetKey , wCompareSetKey ) {
		if ( !wStoreKey ) { return undefined; }
		if ( !wSetKey ) { return undefined; }
		if ( !wCompareSetKey ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.sdiffstore( wStoreKey , wSetKey , wCompareSetKey , function( err , values ) { resolve( values ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});	
	}

	setStoreUnion( wStoreKey , wSetKey1 , wSetKey2  ) {
		if ( !wStoreKey ) { return undefined; }
		if ( !wSetKey1 ) { return undefined; }
		if ( !wSetKey2 ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.sdiffstore( wStoreKey , wSetKey1 , wSetKey2 , function( err , values ) { resolve( values ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});
	}

	// Hashes
	hashSetMulti( ...args ) {
		if ( !args ) { return undefined; }
		let that = this;
		return new Promise( function( resolve , reject ) {
			try { that.redis.hmset( ...args , function( err , values ) { resolve( values ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});
	}

	hashGetAll( wKey ) {
		if ( !wKey ) { return undefined; }
		let that = this;		
		return new Promise( function( resolve , reject ) {
			try { that.redis.hgetall( wKey , function( err , values ) { resolve( values ); }); }
			catch( error ) { console.log( error ); reject( error ); }
		});
	}	

	// Extras
	deleteMultiplePatterns( wKeyPatterns ) {
		if ( !wKeyPatterns ) { return undefined; }
		let that = this;
		return new Promise( async function( resolve , reject ) {
			try {
				let del_keys = await map( wKeyPatterns , wPattern => that.keysGetFromPattern( wPattern ) );
				del_keys = [].concat.apply( [] , del_keys );
				del_keys = del_keys.filter( Boolean );
				
				if ( del_keys ) {
					if ( del_keys.length > 0 ) {
						//console.log( "\nRedis-Manager-Utils --> deleteing these keys --> \n" );
						//console.log( del_keys );						
						del_keys = del_keys.map( x => [ "del" , x  ] );
						await that.keySetMulti( del_keys );
						//console.log( "Redis-Manager-Utils --> done deleteing all keys" );
					}
				}

				resolve();
			}
			catch( error ) { console.log( error ); reject( error ); }
		});
	}

	previousInCircularList( wKey ) {
		if ( !wKey ) { return undefined; }
		let that = this;
		return new Promise( async function( resolve , reject ) {
			try {
				// 1.) Get Length
				let circle_length = await that.listGetLength( wKey );
				if ( !circle_length ) { resolve( "Nothing in Circle List" ); return; }
				//if ( circle_length === 0 ) { resolve( "Nothing in Circle List" ); return; }
				circle_length = parseInt( circle_length );
				console.log( "previousInCircularList( " +  wKey + " )" );
				console.log( "previousInCircularList() --> Length === " + circle_length.toString()  );

				// 2.) Get Previous and Recycle if Necessary
				let previous_index = await that.keyGet( wKey + ".INDEX" );
				console.log( "previousInCircularList() --> Starting Index === " + previous_index.toString() );
				if ( !previous_index ) { previous_index = ( circle_length - 1 ); }
				else { 
					previous_index = ( parseInt( previous_index ) - 1 );
					await that.decrement( wKey + ".INDEX" );
				}
				if ( previous_index < 0 ) {
					previous_index = ( circle_length - 1 );
					console.log( "previousInCircularList() --> " + "Recycling to End of List" );
					await that.keySet( wKey + ".INDEX" , previous_index );
				}
				console.log( "previousInCircularList() --> Ending Index === " + previous_index.toString() );

				const previous_in_circle = await that.listGetByIndex( wKey , previous_index );
				resolve( [ previous_in_circle , previous_index ] );
			}
			catch( error ) { console.log( error ); reject( error ); }
		});
	}
	nextInCircularList( wKey ) {
		if ( !wKey ) { return undefined; }
		let that = this;
		return new Promise( async function( resolve , reject ) {
			try {
				// 1.) Get Length
				let circle_length = await that.listGetLength( wKey );
				if ( !circle_length ) { resolve( "Nothing in Circle List" ); return; }
				//if ( circle_length === 0 ) { resolve( "Nothing in Circle List" ); return; }
				circle_length = parseInt( circle_length );
				//console.log( "Circle Length === " + circle_length.toString() );

				// 2.) Get Next and Recycle if Necessary
				let next_index = await that.keyGet( wKey + ".INDEX" );
				//console.log( "Keys Current Index === " + next_index.toString() );
				if ( !next_index ) { next_index = 0; }
				else { 
					next_index = ( parseInt( next_index ) + 1 );
					await that.increment( wKey + ".INDEX" );
				}
				if ( next_index > ( circle_length - 1 ) ) {
					next_index = 0;
					console.log( "Recycling to Beginning of List" );
				}
				//console.log( "Keys NEXT Index === " + next_index.toString() );

				const next_in_circle = await that.listGetByIndex( wKey , next_index );
				resolve( [ next_in_circle , next_index ] );
			}
			catch( error ) { console.log( error ); reject( error ); }
		});
	}

	// Adds an Array of Potential New Items to a set ,
	// But compares them first to a filter set before adding
	setAddArrayWithFilter( wDestinationKey , wFilterSetKey , wArray ) {
		if ( !wDestinationKey ) { return undefined; }
		if ( !wFilterSetKey ) { return undefined; }
		if ( !wArray ) { return undefined; }
		let that = this;
		return new Promise( async function( resolve , reject ) {
			try {
				const filter_key_exists = await that.exists( wFilterSetKey );
				if ( !filter_key_exists ) { console.log( "Redis-Manager-Utils --> " + wFilterSetKey + " is empty or DNE" ); resolve( wArray ); return; }

				const wTempKey = "TMP_KEY_1." + Math.random().toString(36).substring(7);
				const wTempKey2 = "TMP_KEY_2." + Math.random().toString(36).substring(7);

				// 1.) Store NewVideos into Temp-Random Key
				await that.setSetFromArray( wTempKey , wArray );

				// 2.) Redis StoreDifference
				await that.setAdd( wTempKey2  , "GARBAGE" );
				await that.setSetDifferenceStore( wTempKey2 , wTempKey , wFilterSetKey );
				await that.setRemove( wTempKey2 , "GARBAGE" );

				// 3.) Retrieve The Filtered List
				const filtered_items = await that.setGetFull( wTempKey2 );

				// 4.) Add to Destination Set
				if ( filtered_items ) {
					if ( filtered_items.length > 0 ) {
						await that.setSetFromArray( wDestinationKey , filtered_items );
					}
				}

				// 5.) Cleanup TempKeys
				await that.keyDel( wTempKey );
				await that.keyDel( wTempKey2 );
				resolve( filtered_items );
			}
			catch( error ) { console.log( error ); reject( error ); }
		});
	}	

}

module.exports = RedisUtilsBase