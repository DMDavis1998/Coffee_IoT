use siot

// Insert bad shot docs
db.vwTS_min__NoDeviceTypeWithBody.aggregate( [
    { $sample: { size: 1000 } },
    { $match:  { "body.body.Recipe.ParamCoffee.IsqExtrTime": {$exists: true}}}
]).forEach(function(doc) {
    delete doc._id; 
    doc.body.body.Recipe.ParamCoffee.IsqExtrTime = 1000;
    //print(JSON.stringify(doc));
    db.dataTS_min.insertOne(doc);
});