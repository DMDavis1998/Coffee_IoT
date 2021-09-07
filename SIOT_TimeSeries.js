use siot

// Telemtry only raw
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

// Add timefield and metafield and copy into ts colls
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

// Earliest and Latest timestamp
db.dataTS_sec.aggregate([
    {
        $group: {
            _id: 1,
            minDt: { $min: "$timeField" },
            maxDt: { $max: "$timeField" },
        }
    }
]);



// Questios to answer
- Mark: First and last heartbeat for a device
- Mark: Query the data for a device_id and time range and return a specific message type e.g. heartbeats, telemetry, and a secondary tier would be type of telemetry, e.g. ProductResult, Fault, etc
- How many events for a device under a time range?
- Group(count) event types by device & time range?
- Oven max / min / avg of temperature?
- Get Device Types




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






