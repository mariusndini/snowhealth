var config = {
    "amazon":{
    "UserPoolId": "xxx",
        "ClientId": "xxx"
    },
    "kinesis":{
        "region": "us-east-1",
        "apiVersion": "2015-08-04"
    }
}

const AWS = require('aws-sdk');
var kinesis = new AWS.Firehose(config.kinesis);

exports.handler = async (event) => {
    var json = JSON.stringify(event.body);
    
    kinesis.putRecord({
	Record:{Data: json},
        DeliveryStreamName: 'health-data-stream' //NEED TO PROVIDE EXISTING STREAM FOR DATA TO BE ROUTED
    }, function(err, data) {
        if (err) {
            console.error(err);
        }
        console.log(data);
    });
    
};













