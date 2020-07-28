var config = {
    "amazon":{
    "UserPoolId": "us-east-2_7EEHLBK1g",
        "ClientId": "s3ofc9fgi83k8mnlce3lt0e5v"
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
        DeliveryStreamName: 'health-data-stream'
    }, function(err, data) {
        if (err) {
            console.error(err);
        }
        console.log(data);
    });
    
};













