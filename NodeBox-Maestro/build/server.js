/// <reference path='../typings/node/node.d.ts' />
/// <reference path='../typings/kue/kue.d.ts' />
var net = require('net');
var redis = require('redis');
var kue = require('kue');
var port = 9090;
var server = net.createServer();
var clients = [];
kue.redis.createClient = function () {
    var client = redis.createClient(6379, "localhost");
    return client;
};
var queue = kue.createQueue();
kue.app.listen(3000);
kue.app.set('title', 'Orchestra');
server.on('connection', function (client) {
    clients.push(client);
    client.on('data', function (data) {
        var jsonData = JSON.parse(data);
        var bucketId = jsonData['table'];
        var job = queue.create(bucketId, jsonData);
        job.ttl(10000);
        job.removeOnComplete(true);
        job.save(function (err) {
            if (err) {
                console.log("Error: creating Job");
            }
        });
        job.on('complete', function (result) {
            client.write(JSON.stringify(result));
        });
        job.on('failed attempt', function (errorMessage, doneAttempts) {
            var errorResult = "{ \"success\":\"0\", \"message\":" + errorMessage + "}";
            client.write(errorResult);
        });
        job.on('failed', function (errorMessage) {
            var errorResult = "{ \"success\":\"0\", \"message\":" + errorMessage + "}";
            client.write(errorResult);
        });
    });
});
server.listen(port);
