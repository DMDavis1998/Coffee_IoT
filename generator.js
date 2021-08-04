'use strict';

// Initialize libraries
const mgenerate = require("mgeneratejs");
const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');

main().catch(console.error);

async function main() {

    //const uri = "ATLAS-URI";
    const uri = "mongodb://localhost:27017/Vitals";
    const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

    await client.connect();

    let rawTemplate = fs.readFileSync('template.json');
    let template = JSON.parse(rawTemplate);
    //console.log(template);

    const devices = 100

    var deviceId;
    for (deviceId = 1; deviceId < devices; deviceId++) {
        await insertEventsInBulk(deviceId, client, template)
                .catch(console.error);
    }

    client.close();
}
  
async function insertEventsInBulk(deviceId, client, template) {
    
    const events = 100
    const coll = client.db("Vitals").collection("Events");
    const bulk = coll.initializeUnorderedBulkOp();

    var eventId;
    for (eventId = 1; eventId <= events; eventId++) {

        // Overwrite with specific values
        // ==============================
        
        // Seed template with specific values
        var units = [getRndInt(1000, 9999), getRndInt(5000, 7999), getRndInt(10, 100), getRndInt(50, 80)];
        
        template.device_id = deviceId;
        template.message_id= deviceId+":"+getRndInt(100000, 4999998)+" "+units[0]+" "+units[1]+" "+units[2]+" "+units[3]+":"+new Date().toString();

        // Add new field to template at run time
        template.donsField = deviceId + " for Don";

        // Generate event with random data
        let event = mgenerate(template);
        
        // ==============================
        // Insert
        bulk.insert(event);

        console.log("DEVICE_+ID: " + event.device_id + ". EVENT_ID: " + eventId);
    }

    await bulk.execute();
    
}

function getRndInt(min, max) {
    return mgenerate(
        {
            aNumber: {"$natural":{"min":min, "max":max}}
        }
    ).aNumber;
}