[{$group: {
	_id: 1,
	minObjId: { $min: "$_id" },
	maxObjId: { $max: "$_id" }
  }}]

_id:1
minObjId:6142a380e1837080ba2326f7
maxObjId:6142a3b0e1837080ba24ad96



// =======================
sudo su
nohup bash convertToTS.sh > convertToTS.log &
[1] 50867
[root@downloader bin]# nohup: ignoring input and appending output to 'nohup.out'
// =======================

use siot_1day


/home/snarvaez/mongo50/bin/mongoimport --uri mongodb+srv://snarvaez:snarvaez**@siot-cluster.vbzlz.mongodb.net/siot_1day --collection data --type json --file ./part-00000 --writeConcern 1 -j 4
/home/snarvaez/mongo50/bin/mongoimport --uri mongodb+srv://snarvaez:snarvaez**@siot-cluster.vbzlz.mongodb.net/siot_1day --collection data --type json --file ./part-00001 --writeConcern 1 -j 4
/home/snarvaez/mongo50/bin/mongoimport --uri mongodb+srv://snarvaez:snarvaez**@siot-cluster.vbzlz.mongodb.net/siot_1day --collection data --type json --file ./part-00002 --writeConcern 1 -j 4
/home/snarvaez/mongo50/bin/mongoimport --uri mongodb+srv://snarvaez:snarvaez**@siot-cluster.vbzlz.mongodb.net/siot_1day --collection data --type json --file ./part-00003 --writeConcern 1 -j 4
/home/snarvaez/mongo50/bin/mongoimport --uri mongodb+srv://snarvaez:snarvaez**@siot-cluster.vbzlz.mongodb.net/siot_1day --collection data --type json --file ./part-00004 --writeConcern 1 -j 4
/home/snarvaez/mongo50/bin/mongoimport --uri mongodb+srv://snarvaez:snarvaez**@siot-cluster.vbzlz.mongodb.net/siot_1day --collection data --type json --file ./part-00005 --writeConcern 1 -j 4
/home/snarvaez/mongo50/bin/mongoimport --uri mongodb+srv://snarvaez:snarvaez**@siot-cluster.vbzlz.mongodb.net/siot_1day --collection data --type json --file ./part-00006 --writeConcern 1 -j 4
/home/snarvaez/mongo50/bin/mongoimport --uri mongodb+srv://snarvaez:snarvaez**@siot-cluster.vbzlz.mongodb.net/siot_1day --collection data --type json --file ./part-00007 --writeConcern 1 -j 4
/home/snarvaez/mongo50/bin/mongoimport --uri mongodb+srv://snarvaez:snarvaez**@siot-cluster.vbzlz.mongodb.net/siot_1day --collection data --type json --file ./part-00008 --writeConcern 1 -j 4



var bulk = db.data.initializeUnorderedBulkOp();
bulk.find({}).update(
{ $set: { 
	timeField: new Date(doc.timestamp),
	metaField = {
		message_type: 	doc.message_type,
		device_type:	doc.body.device_type,
		em_id: 			doc.em_id,
		device_id: 		doc.device_id
	}

}});
bulk.find( { item: null } ).update( { $set: { item: "TBD" } } );
bulk.execute();

// Need this for later queries to build TS colls
db.data.createIndex({message_type:1});
db.data.createIndex({device_id:1});
db.data.createIndex({device_type:1});
db.data.createIndex({em_id:1});



db.data.find().forEach(function (doc) {
	var docForTS = doc;
	// Add fields for MongoDB Time Series
	docForTS.timeField = new Date(doc.timestamp);
	docForTS.metaField = {
		em_id: 			doc.em_id,
		store_id:		doc.store_id
    }
	db.dataTS_All_sec.insertOne(docForTS);
});



// Telemetry only raw
db.data.find({ "message_type": "telemetry" }).forEach(function (doc) {
    db.data_telemetryOnly.insertOne(doc);
});
// Heartbeat only raw
db.data.find({ message_type: { $in: ['device_heartbeat', 'edge_heartbeat'] } }).forEach(function (doc) {
    db.data_heartbeatOnly.insertOne(doc);
});

// Time series collections
db.dataTS_sec.drop();
db.dataTS_min.drop();
db.dataTS_hour.drop();

db.createCollection(
    "dataTS_sec",
    {
        timeseries: {
            timeField: "timeField",
            metaField: "metaField",
            granularity: "seconds"
        }
    }
);

db.createCollection(
    "dataTS_min",
    {
        timeseries: {
            timeField: "timeField",
            metaField: "metaField",
            granularity: "minutes"
        }
    }
);

db.createCollection(
    "dataTS_hour",
    {
        timeseries: {
            timeField: "timeField",
            metaField: "metaField",
            granularity: "hours"
        }
    }
);

db.createCollection(
    "dataTS_HB_Device_hour",
    {
        timeseries: {
            timeField: "timeField",
            metaField: "metaField",
            granularity: "hours"
        }
    }
);

db.createCollection(
    "dataTS_HB_Edge_hour",
    {
        timeseries: {
            timeField: "timeField",
            metaField: "metaField",
            granularity: "hours"
        }
    }
);

db.createCollection(
    "dataTS_All_sec",
    {
        timeseries: {
            timeField: "timeField",
            metaField: "metaField",
            granularity: "seconds"
        }
    }
);

// Add timefield and metafield and copy into ts colls
db.data.find().forEach(function (doc) {
	var docForTS = doc;
	// Add fields for MongoDB Time Series
	docForTS.timeField = new Date(doc.timestamp);
	docForTS.metaField = {
		em_id: 			doc.em_id,
		store_id:		doc.store_id
    }
	db.dataTS_All_sec.insertOne(docForTS);
});

db.data.find({ message_type: "device_heartbeat"}).forEach(function (doc) {
	var docForTS = doc;
	// Add fields for MongoDB Time Series
	docForTS.timeField = new Date(doc.timestamp);
	docForTS.metaField = {
		em_id: 			doc.em_id,
		device_id: 		doc.device_id,
		store_id:		doc.store_id
    }
	db.dataTS_HB_Device_hour.insertOne(docForTS);
});

db.data.find({ message_type: "edge_heartbeat"}).forEach(function (doc) {
	var docForTS = doc;
	// Add fields for MongoDB Time Series
	docForTS.timeField = new Date(doc.timestamp);
	docForTS.metaField = {
		em_id: 			doc.em_id,
		store_id:		doc.store_id
    }
	db.dataTS_HB_Edge_hour.insertOne(docForTS);
});

db.data.find({ "message_type": "telemetry" }).forEach(function (doc) {
	var docForTS = doc;
	// Add fields for MongoDB Time Series
	docForTS.timeField = new Date(doc.timestamp);
	docForTS.metaField = {
		message_type: 	doc.message_type,
		device_type:	doc.body.device_type,
		em_id: 			doc.em_id,
		device_id: 		doc.device_id
    }
	// fix types for Conserv HE-3
	if (docForTS.metaField.device_type === "Conserv HE-3") {
		// Fields under body.body
		docForTS.body.body.blend_tds = parseInt(docForTS.body.body.blend_tds);
		docForTS.body.body.inlet_flow = parseFloat(docForTS.body.body.inlet_flow);
		docForTS.body.body.inlet_pressure = parseInt(docForTS.body.body.inlet_pressure);
		docForTS.body.body.inlet_tds = parseInt(docForTS.body.body.inlet_tds);
		docForTS.body.body.outlet_pressure = parseInt(docForTS.body.body.outlet_pressure);
		docForTS.body.body.pdrop1 = parseInt(docForTS.body.body.pdrop1);
		docForTS.body.body.pdrop2 = parseInt(docForTS.body.body.pdrop2);
		docForTS.body.body.recovery = parseInt(docForTS.body.body.recovery);
		docForTS.body.body.rejection = parseInt(docForTS.body.body.rejection);
		docForTS.body.body.ro_pressure = parseInt(docForTS.body.body.ro_pressure);
		docForTS.body.body.ro_pump_cycles = parseInt(docForTS.body.body.ro_pump_cycles);
		docForTS.body.body.tank_counts = parseInt(docForTS.body.body.tank_counts);
		docForTS.body.body.tank_fill_level = parseInt(docForTS.body.body.tank_fill_level);
		docForTS.body.body.tank_pump_cycles = parseInt(docForTS.body.body.tank_pump_cycles);
		docForTS.body.body.tank_pump_pressure = parseInt(docForTS.body.body.tank_pump_pressure);
		docForTS.body.body.waste_tds = parseInt(docForTS.body.body.waste_tds);
	}
	db.dataTS_sec.insertOne(docForTS);
	db.dataTS_min.insertOne(docForTS);
	db.dataTS_hour.insertOne(docForTS);
});


// =====================================
// Convert timestamp from string to date
// =====================================
var i = 0;
var result = null;
var coll = db.data;
var bulk= coll.initializeUnorderedBulkOp();

db.data.find({}).forEach(function (doc) {

	if (Date.parse(doc.timestamp) != NaN) {
		bulk.find( { _id: doc._id } )
			.updateOne( { $set: { timestamp : new Date(doc.timestamp) }} )
		i++;

		if (i % 100000 == 0) {
			try {
				result = bulk.execute({w:1}, {ordered:false});
				print (i);
				print (result);			
			} catch (e) {
				print (e);
			} finally {
				bulk= coll.initializeUnorderedBulkOp();
			}
		}
	}
});

try {
	var result = bulk.execute({w:1}, {ordered:false});
	print (i + " [" + Date.now().toString() + "]");
	print (result);			
} catch (e) {
	print (e);
} finally {
	bulk= coll.initializeUnorderedBulkOp();
}

print (i + " [" + Date.now().toString() + "]");
print (result);
print("DONE");
print();




// =========================
// NEW TS COLL (just one)
// =========================
db.dataTS_telemetry_sec.drop();
db.createCollection(
    "dataTS_telemetry_sec",
    {
        timeseries: {
            timeField: "timeField",
            metaField: "metaField",
            granularity: "seconds"
        }
    }
);

db.dataTS_telemetry_min.drop();
db.createCollection(
    "dataTS_telemetry_min",
    {
        timeseries: {
            timeField: "timeField",
            metaField: "metaField",
            granularity: "minutes"
        }
    }
);

db.dataTS_telemetry_hour.drop();
db.createCollection(
    "dataTS_telemetry_hour",
    {
        timeseries: {
            timeField: "timeField",
            metaField: "metaField",
            granularity: "hours"
        }
    }
);

var colls = [
	db.dataTS_telemetry_sec
];

colls.forEach(function(coll) {
	var i = 0;
	var result = null;
	var bulk= coll.initializeUnorderedBulkOp();
	var query = {
		"message_type": "telemetry"
	}
	var sort = {
		"message_type": 1,
		"device_id": 1,
		"timestamp": 1
	}

	db.data.find(query).sort(sort).forEach(function (doc) {
		var docForTS = doc;	
		// Add fields for MongoDB Time Series
		docForTS.timeField = new Date(doc.timestamp);
		// Move static fields to metafield
		docForTS.metaField = {
			device_id:		doc.device_id,
			em_id:			doc.em_id,
			store_id:		doc.store_id,
			storeid:		doc.storeid,
			fleets:			doc.fleets,	
			timezone:		doc.timezone,
			topic:			doc.topic,
			message_type:	doc.message_type,
			device_type:	doc.device_type
		}
		// Remove from main body
		delete docForTS.timestamp;
		delete docForTS.device_id;
		delete docForTS.em_id;
		delete docForTS.store_id;
		delete docForTS.storeid;
		delete docForTS.fleets;
		delete docForTS.timezone;
		delete docForTS.topic;
		delete docForTS.message_type;
		delete docForTS.device_type;

		bulk.insert(docForTS);
		i++;

		if (i % 100000 == 0) {
			try {
				result = bulk.execute({w:1}, {ordered:false});
				print (i);
				print (result);			
			} catch (e) {
				print (e);
			} finally {
				bulk= coll.initializeUnorderedBulkOp();
			}
		}
	});

	try {
	var result = bulk.execute({w:1}, {ordered:false});
	print (i);
	print (result);			
	} catch (e) {
		print (e);
	} finally {
		bulk= coll.initializeUnorderedBulkOp();
	}

	print (i);
	print (result);
	print("DONE");
	print();
});

colls.forEach(function(coll) {
	coll.stats().timeseries;
}


// =====================
// ISQ calculation
// =====================
/*
device_type == 'mastrena'
body.type == 'ProductResult'
body.body.Result.ResultCoffee.Valid == true
body.id does not start with @ or _
body.body.Result.ResultCoffee.ErrorNo != 29
body.body.Result.ResultCoffee.BrewCycle{N}.Valid for at least 1 where N is 1...5
body.body.Result.ResultCoffee.BrewCycleN.PuckThicknAfterSq > 0
*/

// =====================
// Partial index for ISQ calcultion
// =====================
db.data.createIndex(
	{
		"device_id": 1,
		"timestamp": 1
	},
	{ 	name: "IX_ISQ_CALC",
		partialFilterExpression: { 
			"device_type": "mastrena",
			"message_type": "telemetry",
			"body.type": "ProductResult",
			"body.body.Result.ResultCoffee.Valid": true,
		}
	}
 );

// Marks ISQ calculation
db.data.find(
{	
	"device_id": "410004617",
	"timestamp": {"$gte":new Date("2021-09-14T00:00:00.000"), "$lte":new Date("2021-09-14T23:59:59.999")},
	"device_type": "mastrena",
	"body.type": "ProductResult",
	"message_type": "telemetry",
	"body.body.Result.ResultCoffee": {
		"$exists": true
	},
	"body.body.Result.ResultCoffee.ErrorNo": {
		"$exists": true
	},
	"body.body.Result.ResultCoffee.ErrorNo": {
		"$ne": 29
	},
	"body.body.Result.ResultCoffee.Valid": true,
	"$or": [{
			"body.body.Result.ResultCoffee.BrewCycle1.Valid": true,
			"body.body.Result.ResultCoffee.BrewCycle1.PuckThicknAfterSq": {"$gt": 0}
		},
		{
			"body.body.Result.ResultCoffee.BrewCycle2.Valid": true,
			"body.body.Result.ResultCoffee.BrewCycle2.PuckThicknAfterSq": {"$gt": 0}
		},
		{
			"body.body.Result.ResultCoffee.BrewCycle3.Valid": true,
			"body.body.Result.ResultCoffee.BrewCycle3.PuckThicknAfterSq": {"$gt": 0}
		},
		{
			"body.body.Result.ResultCoffee.BrewCycle4.Valid": true,
			"body.body.Result.ResultCoffee.BrewCycle4.PuckThicknAfterSq": {"$gt": 0}
		},
		{
			"body.body.Result.ResultCoffee.BrewCycle5.Valid": true,
			"body.body.Result.ResultCoffee.BrewCycle5.PuckThicknAfterSq": {"$gt": 0}
		}
	]
}).hint("IX_ISQ_CALC");

// Aggregation
db.date.aggregate(
[{$match: {	
	"device_id": "410004617",
	"timestamp": {"$gte":new Date("2021-09-14T00:00:00.000"), "$lte":new Date("2021-09-14T23:59:59.999")},
	"device_type": "mastrena",
	"body.type": "ProductResult",
	"message_type": "telemetry",
	"body.body.Result.ResultCoffee": {
		"$exists": true
	},
	"body.body.Result.ResultCoffee.ErrorNo": {
		"$exists": true
	},
	"body.body.Result.ResultCoffee.ErrorNo": {
		"$ne": 29
	},
	"body.body.Result.ResultCoffee.Valid": true,
	"$or": [{
			"body.body.Result.ResultCoffee.BrewCycle1.Valid": true,
			"body.body.Result.ResultCoffee.BrewCycle1.PuckThicknAfterSq": {"$gt": 0}
		},
		{
			"body.body.Result.ResultCoffee.BrewCycle2.Valid": true,
			"body.body.Result.ResultCoffee.BrewCycle2.PuckThicknAfterSq": {"$gt": 0}
		},
		{
			"body.body.Result.ResultCoffee.BrewCycle3.Valid": true,
			"body.body.Result.ResultCoffee.BrewCycle3.PuckThicknAfterSq": {"$gt": 0}
		},
		{
			"body.body.Result.ResultCoffee.BrewCycle4.Valid": true,
			"body.body.Result.ResultCoffee.BrewCycle4.PuckThicknAfterSq": {"$gt": 0}
		},
		{
			"body.body.Result.ResultCoffee.BrewCycle5.Valid": true,
			"body.body.Result.ResultCoffee.BrewCycle5.PuckThicknAfterSq": {"$gt": 0}
		}
	]
}}, {$group: {
	_id: "$device_id",
	avgBrewCycle1_ExtrTime:         { "$avg": "$body.body.Result.ResultCoffee.BrewCycle1.ExtrTime"},
	avgBrewCycle1_PuckThicknAfterPr:{ "$avg": "$body.body.Result.ResultCoffee.BrewCycle1.PuckThicknAfterPr"},
	avgBrewCycle1_PuckThicknAfterSq:{ "$avg": "$body.body.Result.ResultCoffee.BrewCycle1.PuckThicknAfterSq"},

	avgBrewCycle2_ExtrTime:         { "$avg": "$body.body.Result.ResultCoffee.BrewCycle2.ExtrTime"},
	avgBrewCycle2_PuckThicknAfterPr:{ "$avg": "$body.body.Result.ResultCoffee.BrewCycle2.PuckThicknAfterPr"},
	avgBrewCycle2_PuckThicknAfterSq:{ "$avg": "$body.body.Result.ResultCoffee.BrewCycle2.PuckThicknAfterSq"},

	avgBrewCycle3_ExtrTime:         { "$avg": "$body.body.Result.ResultCoffee.BrewCycle3.ExtrTime"},
	avgBrewCycle3_PuckThicknAfterPr:{ "$avg": "$body.body.Result.ResultCoffee.BrewCycle3.PuckThicknAfterPr"},
	avgBrewCycle3_PuckThicknAfterSq:{ "$avg": "$body.body.Result.ResultCoffee.BrewCycle3.PuckThicknAfterSq"},

	avgBrewCycle4_ExtrTime:         { "$avg": "$body.body.Result.ResultCoffee.BrewCycle4.ExtrTime"},
	avgBrewCycle4_PuckThicknAfterPr:{ "$avg": "$body.body.Result.ResultCoffee.BrewCycle4.PuckThicknAfterPr"},
	avgBrewCycle4_PuckThicknAfterSq:{ "$avg": "$body.body.Result.ResultCoffee.BrewCycle4.PuckThicknAfterSq"},

	avgBrewCycle5_ExtrTime:         { "$avg": "$body.body.Result.ResultCoffee.BrewCycle5.ExtrTime"},
	avgBrewCycle5_PuckThicknAfterPr:{ "$avg": "$body.body.Result.ResultCoffee.BrewCycle5.PuckThicknAfterPr"},
	avgBrewCycle5_PuckThicknAfterSq:{ "$avg": "$body.body.Result.ResultCoffee.BrewCycle5.PuckThicknAfterSq"}
}}], {allowDiskUse:true});




// Compare stats after loading
db.data.stats();
db.data_telemetryOnly.stats();
data.data_heartbeatOnly.stats();

db.system.buckets.dataTS_sec.stats();
db.system.buckets.dataTS_min.stats();
db.system.buckets.dataTS_hour.stats();

// Create Views
//MERRYCHEF
db.createView(
	"vwTS_min__MERRYCHEF",
	"dataTS_min",
	[ { $match: {'metaField.device_type': 'MERRYCHEF'} } ]
);
//Conserv HE-3
db.createView(
	"vwTS_min__ConservHE-3",
	"dataTS_min",
	[ { $match: {'metaField.device_type': 'Conserv HE-3'} } ]
);
//compact_nitro_gen2.2
db.createView(
	"vwTS_min__CompactNitroGen22",
	"dataTS_min",
	[ { $match: {'metaField.device_type': 'compact_nitro_gen2.2'} } ]
);
//No Device Type, but body.body is provided
db.createView(
	"vwTS_min__NoDeviceTypeWithBody",
	"dataTS_min",
	[ { $match: {'body.device_type': { $exists: false }, 'body.body': { $exists: true }} } ]
);

// ===========================================
// Peak messages / sec
// ===========================================
db.dataTS_All_sec.aggregate(
[{$project: {
	tf: { $dateToParts: {date: "$timeField"}}
  }}, {$addFields: {
	tfToSecs: {
	  $dateFromParts : {
		  year: "$tf.year", 
		  month: "$tf.month",  
		  day: "$tf.day", 
		  hour: "$tf.hour", 
		  minute: "$tf.minute",
		  second: "$tf.second"
	  }
  }
  }}, {$group: {
	_id: "$tfToSecs",
	peak: {
	  $sum: 1
	}
  }}, {$sort: {
	peak: -1
  }}]);
// Result
// _id:2021-09-02T12:35:31.000+00:00
// peak:778

// ===========================================
// Peak Edge HB’s/Sec
// ===========================================
db.dataTS_HB_Edge_hour.aggregate(
	[{$project: {
		tf: { $dateToParts: {date: "$timeField"}}
	  }}, {$addFields: {
		tfToSecs: {
		  $dateFromParts : {
			  year: "$tf.year", 
			  month: "$tf.month",  
			  day: "$tf.day", 
			  hour: "$tf.hour", 
			  minute: "$tf.minute",
			  second: "$tf.second"
		  }
	  }
	  }}, {$group: {
		_id: "$tfToSecs",
		peak: {
		  $sum: 1
		}
	  }}, {$sort: {
		peak: -1
	  }}]);
_id:2021-09-02T13:35:35.000+00:00
peak:63

// ===========================================
// Peak Device HB’s/Sec
// ===========================================
db.dataTS_HB_Device_hour.aggregate(
	[{$project: {
		tf: { $dateToParts: {date: "$timeField"}}
	  }}, {$addFields: {
		tfToSecs: {
		  $dateFromParts : {
			  year: "$tf.year", 
			  month: "$tf.month",  
			  day: "$tf.day", 
			  hour: "$tf.hour", 
			  minute: "$tf.minute",
			  second: "$tf.second"
		  }
	  }
	  }}, {$group: {
		_id: "$tfToSecs",
		peak: {
		  $sum: 1
		}
	  }}, {$sort: {
		peak: -1
	  }}]);
_id:2021-09-02T12:35:31.000+00:00
peak:549

// ===========================================
// Peak Telemetry Msgs/Sec 
// ===========================================
db.dataTS_hour.aggregate(
	[{$project: {
		tf: { $dateToParts: {date: "$timeField"}}
	  }}, {$addFields: {
		tfToSecs: {
		  $dateFromParts : {
			  year: "$tf.year", 
			  month: "$tf.month",  
			  day: "$tf.day", 
			  hour: "$tf.hour", 
			  minute: "$tf.minute",
			  second: "$tf.second"
		  }
	  }
	  }}, {$group: {
		_id: "$tfToSecs",
		peak: {
		  $sum: 1
		}
	  }}, {$sort: {
		peak: -1
	  }}]);
_id:2021-09-02T13:35:32.000+00:00
peak:228

// ===========================================
// Timeperiod - Earliest and Latest timestamp
// ===========================================
db.dataTS_min.aggregate([
	{ $group: {
		_id: 1,
		minTS: { $min: "$timeField" },
		maxTS: { $max: "$timeField" },
	}},
	{ $addFields: {
		totalMins: { $dateDiff: {startDate: "$minTS", endDate: "$maxTS", unit: "minute"}}
	}}
])
// RESULT
/*
	_id: 1,
	minTS: ISODate("2021-09-02T12:05:31.000Z"),
	maxTS: ISODate("2021-09-02T14:21:15.000Z"),
	totalMins: 136
*/

// Unique Device ID's & Unique EM ID's
// Unique EM ID's
[{$group: {
	_id: 1,
	uniqueEM_IDs: {
	  $addToSet: "$em_id"
	},
	uniqueDevice_IDs: {
	  $addToSet: "$device_id"
	}
  }}, {$project: {
	uniqueEM_IDs: {$size: "$uniqueEM_IDs"},
	uniqueDevice_IDs: {$size: "$uniqueDevice_IDs"},
  }}]
_id:1
uniqueEM_IDs:15170
uniqueDevice_IDs:13123


// Heartbeats - devices
db.data.aggregate([
{$match: {
	message_type: { $in: ['device_heartbeat', 'edge_heartbeat'] } 
  }
  }, {$group: {
	_id: 1,
	deviceIds: {$addToSet: "$device_id"}
  }}, {$project: {
	totalDevices: {$size:"$deviceIds"}
  }}
]);
// _id:1
// totalDevices sending HB's: 7117 
// totalDevices sending Telemtry: 9969

// Unique Devices
db.dataTS_All_sec.aggregate(
[{$group: {
	_id: 1,
	uniqueEM_IDs: {
	  $addToSet: "$em_id"
	},
	uniqueDevice_IDs: {
	  $addToSet: "$device_id"
	}
  }}, {$project: {
	uniqueEM_IDs: {$size: "$uniqueEM_IDs"},
	uniqueDevice_IDs: {$size: "$uniqueDevice_IDs"},
  }}]);
  _id:1
  uniqueEM_IDs:15170
  uniqueDevice_IDs:13123 

// Unique Stores
[{$group: {
	_id: 1,
	store_ids: {$addToSet: "$store_id"}
}}, {$project: {
	uniqueStore_ids: {$size:"$store_ids"}
}}]
_id:1
uniqueStore_ids:7335

// Peak messages/sec
db.dataTS_min.aggregate([
	{$group: {
		_id: "$timeField",
		totalHBs: {$sum: 1}
	}}, 
	{$sort: {
		totalHBs: -1
	}},
	{$limit: 1}
]);
//[ { _id: ISODate("2021-09-02T13:35:32.000Z"), totalHBs: 228 } ]

// Peak device HB/sec
db.dataTS_HB_Device_hour.aggregate([
	{$group: {
		_id: "$timeField",
		totalHBs: {$sum: 1}
	}}, 
	{$sort: {
		totalHBs: -1
	}},
	{$limit: 1}
]);
//[ { _id: ISODate("2021-09-02T12:35:31.000Z"), totalHBs: 549 } ]

// Peak edge HB/sec
db.dataTS_HB_Edge_hour.aggregate([
	{$group: {
		_id: "$timeField",
		totalHBs: {$sum: 1}
	}}, 
	{$sort: {
		totalHBs: -1
	}},
	{$limit: 1}
]);
//[ { _id: ISODate("2021-09-02T13:35:35.000Z"), totalHBs: 63 } ]

// Peak Telemetry/sec


// Heartbeats over Timeperiod


// Average HB per device
// Device with most HB's

// Telemtry - devices
db.data.aggregate([
	{$match: {
		message_type: { $in: ['telemetry'] } 
	  }
	  }, {$group: {
		_id: 1,
		deviceIds: {$addToSet: "$device_id"}
	  }}, {$project: {
		totalDevices: {$size:"$deviceIds"}
	  }}
	]);
	// _id:1
	// totalDevices:9969

// Telemetry over Timeperiod
// Average telemtry payloads per device
// Device with most payloads



// 1) First and last heartbeat
db.createView(
	"vwTS_min__DeviceFirstLastHB",
	"dataTS_min",
	[
		{'$group': {
			'_id': {
				'device_type': '$metaField.device_type', 
				'device_id': '$metaField.device_id'
			}, 
			'firstHB': {
				'$min': '$timeField'
			}, 
			'lastHB': {
				'$max': '$timeField'
			}
		}}
	]
);

// Query only valid device types
db.vwTS_min__DeviceFirstLastHB.find({"_id.device_type":{$exists: true, $ne: null}});

// Aggregations 

db.vwTS_min__NoDeviceTypeWithBody.aggregate( [
	{ $match: {
		"body.body.Recipe.ParamCoffee.IsqExtrTime" : {"$gt": 0},
		"body.body.Recipe.ParamCoffee.MaxValidExtrTime" : {"$ne":3600}, 
		"body.body.Recipe.ParamCoffee.MinValidExtrTime" : {"$ne":-1 } 
	}}
]);

db.vwTS_min__NoDeviceTypeWithBody.aggregate( [
	{ $match: {
		"body.body.Recipe.ParamCoffee.IsqExtrTime" : {"$gt": 0},
		"body.body.Recipe.ParamCoffee.MaxValidExtrTime" : {"$ne":3600}, 
		"body.body.Recipe.ParamCoffee.MinValidExtrTime" : {"$ne":-1 } 
	}},
	{
	  $project:
		{
		  _id:			0,
		  "fleet": 		{ $arrayElemAt: [ "$fleets", 0 ] },
		  "timeField": 	1,
		  "device_id":	"$metaField.device_id",
		  "IsqExtrTime":		"$body.body.Recipe.ParamCoffee.IsqExtrTime",
		  "MaxValidExtrTime":	"$body.body.Recipe.ParamCoffee.MaxValidExtrTime",
		  "MinValidExtrTime":	"$body.body.Recipe.ParamCoffee.MinValidExtrTime"
		}
	},
	{
		$addFields: {
			"GoodShot": {
				$and: [
					{$gte: ["$IsqExtrTime", "$MinValidExtrTime"]}, 
					{$lte: ["$IsqExtrTime", "$MaxValidExtrTime"]}
				]
			}
		  }
	}
]);

db.createView(
	"vwTS_min__GoodShotBadShot",
	"vwTS_min__NoDeviceTypeWithBody",
	[
		{ $match: {
			"body.body.Recipe.ParamCoffee.IsqExtrTime" : {"$gt": 0},
			"body.body.Recipe.ParamCoffee.MaxValidExtrTime" : {"$ne":3600}, 
			"body.body.Recipe.ParamCoffee.MinValidExtrTime" : {"$ne":-1 } 
		}},
		{
		  $project:
			{
			  _id:			0,
			  "fleet": 		{ $arrayElemAt: [ "$fleets", 0 ] },
			  "timeField": 	1,
			  "device_id":	"$metaField.device_id",
			  "IsqExtrTime":		"$body.body.Recipe.ParamCoffee.IsqExtrTime",
			  "MaxValidExtrTime":	"$body.body.Recipe.ParamCoffee.MaxValidExtrTime",
			  "MinValidExtrTime":	"$body.body.Recipe.ParamCoffee.MinValidExtrTime"
			}
		},
		{
			$addFields: {
				"GoodShotBool": {
					$and: [
						{$gte: ["$IsqExtrTime", "$MinValidExtrTime"]}, 
						{$lte: ["$IsqExtrTime", "$MaxValidExtrTime"]}
					]
				}
			  }
		},
		{
			$addFields: {
				"GoodShotNum": {
					$switch:
					{
					branches: [
						{
						case: {$and: [
							{$gte: ["$IsqExtrTime", "$MinValidExtrTime"]}, 
							{$lte: ["$IsqExtrTime", "$MaxValidExtrTime"]}
						]},
						then: 1
						},
					],
					default: 0
					}
				}
		}}
	]
);

// Questios to answer
- Mark: First and last heartbeat for a device
- Mark: Query the data for a device_id and time range and return a specific message type e.g. heartbeats, telemetry, and a secondary tier would be type of telemetry, e.g. ProductResult, Fault, etc
- How many events for a device under a time range?
- Group(count) event types by device & time range?
- Oven max / min / avg of temperature?
- Get Device Types



/*

Mark Quilling  9:13 PM
i could actually use your help with something now that you have data for 9.14
how many total ProductResults with body.body.Recipe.ParamCoffee.UseForISQ == true does device 410014178 have in that day (note the day boundary is in the timezone, so timestamps need conversion before binning into a day) ?
these are the kinds of queries we will do for debugging and validation
*/
[{$match: {
	"device_id": "410014178", 
	"body.type":"ProductResult", 
	"body.body.Recipe.ParamCoffee.UseForISQ":true
  }}, {$project: {
	_id:			0,
	"fleet": 		{ $arrayElemAt: [ "$fleets", 0 ] },
	"timeField": 	1,
	"device_id":	1,
	"IsqExtrTime":		"$body.body.Recipe.ParamCoffee.IsqExtrTime",
	"MaxValidExtrTime":	"$body.body.Recipe.ParamCoffee.MaxValidExtrTime",
	"MinValidExtrTime":	"$body.body.Recipe.ParamCoffee.MinValidExtrTime"
  }}, {$addFields: {
	"GoodShot": {
				$and: [
					{$gte: ["$IsqExtrTime", "$MinValidExtrTime"]}, 
					{$lte: ["$IsqExtrTime", "$MaxValidExtrTime"]}
				]
			}
  }}, {$group: {
	_id: "$device_id",
	IsqExtrTime_MIN: { $min: "$IsqExtrTime" },
	IsqExtrTime_AVG: { $avg: "$IsqExtrTime" },
	IsqExtrTime_MAX: { $max: "$IsqExtrTime" }
  
	}}]

// Manual analysis & fields found by device type
/*
{ 'body.device_type': 'MERRYCHEF' }
13814/28017 = 49.3%
// Fields under body.body
cavityTemp: 4985
door_cycle: 31760
doorStatus: "Door Closed"
event: "oven information"
event: "ovenstatus"
filter_cycle: 134
heater_ontime: 7478290
left_magnetron_ontime: 381448
oven_poweron_time: 7648136
ovenStatus: "Ready to Cook"
preHeat: 788
right_magnetron_ontime: 388025
totalcookcount: 2345

{ 'body.device_type': "Conserv HE-3" }
125/28017 = 0.44%
// Fields under body.body
blend_tds: "95"
inlet_flow: "0.39"
inlet_pressure: "105"
inlet_tds: "213"
outlet_pressure: "30"
pdrop1: "1"
pdrop2: "0"
recovery: "83"
rejection: "83"
ro_pressure: "99"
ro_pump_cycles: "45411"
tank_counts: "912"
tank_fill_level: "55"
tank_pump_cycles: "272806"
tank_pump_pressure: "39"
waste_tds: "416"
// TO-DO: Convert strings to values

{ 'body.device_type': "compact_nitro_gen2.2" }
Population: 1 document
// Fields under body.body
current_amps: 0
event: "data"
pressure_psi: 118.93
temperature_deg_f: 91.97

{ "body.device_type": { $exists: false }, "body.body": { $exists: true } }
Population: 13949/28017 = 50%
// Fields under body.body
"body": {
	"body": {
		"Recipe": {
			"Category": "Espresso",
			"ParamAmount": {
				"Module": "",
				"Name": "IngredientParametersAmount",
				"Temperature": -1,
				"Valid": false,
				"WaterAmount": 0
			},
			"ParamAutoOut": {
				"CupHeightMillimeters": 70,
				"CupIdentifier": "",
				"Module": "",
				"Name": "IngredientParametersAutoOutlet",
				"Valid": false
			},
			"ParamCoffee": {
				"Grinder1": {
					"Amount": 19,
					"Valid": true
				},
				"Grinder2": {
					"Amount": 0,
					"Valid": false
				},
				"Grinder3": {
					"Amount": 0,
					"Valid": false
				},
				"IsqExtrTime": 0,
				"IsqPuckThickn": 0,
				"MaxExtrTime": -1,
				"MaxValidExtrTime": 50,
				"MinValidExtrTime": 5,
				"Module": "",
				"Name": "Coffee",
				"NumBrewCycles": 1,
				"PostPress": 0,
				"PreBrew": 0,
				"PressFactor": 0.6,
				"RelaxTime": 0.1,
				"TotalAmount": 110,
				"UseExpChamber": true,
				"UseForISQ": false,
				"Valid": true
			},
			"ParamCoffeeGrind": {
				"Grinder1": {
					"Amount": 0,
					"Valid": false
				},
				"Grinder2": {
					"Amount": 0,
					"Valid": false
				},
				"Grinder3": {
					"Amount": 0,
					"Valid": false
				},
				"Module": "",
				"Valid": false
			},
			"ParamFlavor": {
				"CheckFlavorFrontDoorReed": true,
				"Flavor1": {
					"AmountSeconds": 0,
					"Valid": false
				},
				"Flavor2": {
					"AmountSeconds": 0,
					"Valid": false
				},
				"Flavor3": {
					"AmountSeconds": 0,
					"Valid": false
				},
				"Flavor4": {
					"AmountSeconds": 0,
					"Valid": false
				},
				"Module": "",
				"Name": "IngredientParametersFlavor",
				"Valid": false
			},
			"ParamMilk": {
				"MilkPhase1": {
					"AirAmount": 0,
					"Duration": 0,
					"PumpSpeed": 0,
					"Valid": false
				},
				"MilkPhase10": {
					"AirAmount": 0,
					"Duration": 0,
					"PumpSpeed": 0,
					"Valid": false
				},
				"MilkPhase2": {
					"AirAmount": 0,
					"Duration": 0,
					"PumpSpeed": 0,
					"Valid": false
				},
				"MilkPhase3": {
					"AirAmount": 0,
					"Duration": 0,
					"PumpSpeed": 0,
					"Valid": false
				},
				"MilkPhase4": {
					"AirAmount": 0,
					"Duration": 0,
					"PumpSpeed": 0,
					"Valid": false
				},
				"MilkPhase5": {
					"AirAmount": 0,
					"Duration": 0,
					"PumpSpeed": 0,
					"Valid": false
				},
				"MilkPhase6": {
					"AirAmount": 0,
					"Duration": 0,
					"PumpSpeed": 0,
					"Valid": false
				},
				"MilkPhase7": {
					"AirAmount": 0,
					"Duration": 0,
					"PumpSpeed": 0,
					"Valid": false
				},
				"MilkPhase8": {
					"AirAmount": 0,
					"Duration": 0,
					"PumpSpeed": 0,
					"Valid": false
				},
				"MilkPhase9": {
					"AirAmount": 0,
					"Duration": 0,
					"PumpSpeed": 0,
					"Valid": false
				},
				"Module": "",
				"Name": "IngredientParametersMilk",
				"PureFoam": false,
				"Temperature": 0,
				"Valid": false
			},
			"ParamPowder": {
				"Module": "",
				"Name": "IngredientParametersPowder",
				"PowderAmountSeconds": 0,
				"PowderIntensity": 0,
				"Temperature": -1,
				"Valid": false
			},
			"ParamSteam": {
				"AutoOff": false,
				"Module": "",
				"Temperature": -1,
				"Valid": false
			}
		},
		"Result": {
			"ResultAmount": {
				"ErrorNo": 0,
				"Module": "",
				"Name": "",
				"Success": false,
				"Valid": false,
				"WaterAmount": 0
			},
			"ResultCoffee": {
				"BrewChamberIdx": 0,
				"BrewCycle1": {
					"ExtrTime": 0,
					"PuckThicknAfterPr": 0,
					"PuckThicknAfterSq": 0,
					"Valid": false
				},
				"BrewCycle2": {
					"ExtrTime": 0,
					"PuckThicknAfterPr": 0,
					"PuckThicknAfterSq": 0,
					"Valid": false
				},
				"BrewCycle3": {
					"ExtrTime": 0,
					"PuckThicknAfterPr": 0,
					"PuckThicknAfterSq": 0,
					"Valid": false
				},
				"BrewCycle4": {
					"ExtrTime": 0,
					"PuckThicknAfterPr": 0,
					"PuckThicknAfterSq": 0,
					"Valid": false
				},
				"BrewCycle5": {
					"ExtrTime": 0,
					"PuckThicknAfterPr": 0,
					"PuckThicknAfterSq": 0,
					"Valid": false
				},
				"ErrorNo": 29,
				"Grinder1": {
					"Adjustment": 0,
					"CalibFactor": 54.8530563293,
					"Duration": 3.3619615169570967,
					"Rate": 3.1,
					"Valid": true
				},
				"Grinder2": {
					"Adjustment": 0,
					"CalibFactor": 0,
					"Duration": 0,
					"Rate": 0,
					"Valid": false
				},
				"Grinder3": {
					"Adjustment": 0,
					"CalibFactor": 0,
					"Duration": 0,
					"Rate": 0,
					"Valid": false
				},
				"InletWaterCond": -1,
				"InletWaterTemp": -273.15,
				"Module": "CoffeeModule1",
				"Name": "Coffee",
				"NumBrewCycles": 0,
				"Success": false,
				"Valid": true,
				"WaterAmount": 0
			},
			"ResultCoffeeClean": {
				"ErrorNo": 0,
				"Module": "",
				"Success": false,
				"Valid": false,
				"WaterAmount": 0
			},
			"ResultCoffeeGrind": {
				"ErrorNo": 0,
				"Grinder1": {
					"Adjustment": 0,
					"CalibFactor": 0,
					"Duration": 0,
					"Rate": 0,
					"Valid": false
				},
				"Grinder2": {
					"Adjustment": 0,
					"CalibFactor": 0,
					"Duration": 0,
					"Rate": 0,
					"Valid": false
				},
				"Grinder3": {
					"Adjustment": 0,
					"CalibFactor": 0,
					"Duration": 0,
					"Rate": 0,
					"Valid": false
				},
				"Module": "",
				"Success": false,
				"Valid": false
			},
			"ResultCoffeeRinse1": {
				"ErrorNo": 0,
				"Name": "",
				"Success": false,
				"Valid": false,
				"WaterAmount": 0
			},
			"ResultCoffeeRinse2": {
				"ErrorNo": 0,
				"Name": "",
				"Success": false,
				"Valid": false,
				"WaterAmount": 0
			},
			"ResultFlavor": {
				"ErrorNo": 0,
				"Module": "",
				"Name": "",
				"Success": false,
				"Valid": false
			},
			"ResultMilk": {
				"ErrorNo": 0,
				"MilkPhase1": {
					"Duration": 0,
					"PumpSpeedRPM": 0,
					"Valid": false
				},
				"MilkPhase10": {
					"Duration": 0,
					"PumpSpeedRPM": 0,
					"Valid": false
				},
				"MilkPhase2": {
					"Duration": 0,
					"PumpSpeedRPM": 0,
					"Valid": false
				},
				"MilkPhase3": {
					"Duration": 0,
					"PumpSpeedRPM": 0,
					"Valid": false
				},
				"MilkPhase4": {
					"Duration": 0,
					"PumpSpeedRPM": 0,
					"Valid": false
				},
				"MilkPhase5": {
					"Duration": 0,
					"PumpSpeedRPM": 0,
					"Valid": false
				},
				"MilkPhase6": {
					"Duration": 0,
					"PumpSpeedRPM": 0,
					"Valid": false
				},
				"MilkPhase7": {
					"Duration": 0,
					"PumpSpeedRPM": 0,
					"Valid": false
				},
				"MilkPhase8": {
					"Duration": 0,
					"PumpSpeedRPM": 0,
					"Valid": false
				},
				"MilkPhase9": {
					"Duration": 0,
					"PumpSpeedRPM": 0,
					"Valid": false
				},
				"Module": "",
				"Name": "",
				"Success": false,
				"Valid": false,
				"WaterAmount": 0
			},
			"ResultMilkClean": {
				"ErrorNo": 0,
				"Module": "",
				"Name": "",
				"Rinse1": {
					"Duration": 0,
					"Valid": false,
					"WaterAmount": 0
				},
				"Rinse2": {
					"Duration": 0,
					"Valid": false,
					"WaterAmount": 0
				},
				"Rinse3": {
					"Duration": 0,
					"Valid": false,
					"WaterAmount": 0
				},
				"Rinse4": {
					"Duration": 0,
					"Valid": false,
					"WaterAmount": 0
				},
				"Rinse5": {
					"Duration": 0,
					"Valid": false,
					"WaterAmount": 0
				},
				"Success": false,
				"Valid": false,
				"WaterAmount": 0
			},
			"ResultMilkRinse": {
				"ErrorNo": 0,
				"Module": "",
				"Name": "",
				"Success": false,
				"Valid": false,
				"WaterAmount": 0
			},
			"ResultPowder": {
				"ErrorNo": 0,
				"Module": "",
				"Name": "",
				"Success": false,
				"Valid": false,
				"WaterAmount": 0
			},
			"ResultPowderClean": {
				"ErrorNo": 0,
				"Module": "",
				"Name": "",
				"Success": false,
				"Valid": false,
				"WaterAmount": 0
			},
			"ResultPowderRinse": {
				"ErrorNo": 0,
				"Module": "",
				"Name": "",
				"Success": false,
				"Valid": false,
				"WaterAmount": 0
			},
			"ResultSteam": {
				"AutoStop": false,
				"Duration": 0,
				"EndTemp": 0,
				"ErrorNo": 0,
				"ManualStop": false,
				"Module": "",
				"Success": false,
				"Valid": false
			},
			"Success": false
		},
		"articleno": 0,
		"id": "Blonde Esp_Blonde_Triple_Regular_NotUpdosed_Undefined_Undefined",
		"name": "Blonde Blonde Esp (3 - -)",
		"price": 0
	},
	"eventTime": "2021-09-02T05:05:49-07:00",
	"eventTimeLocal": "2021-09-02T05:05:49",
	"type": "ProductResult",
	"version": "1.9.0"
}

*/






