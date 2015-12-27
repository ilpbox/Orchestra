/// <reference path='../typings/node/node.d.ts' />
/// <reference path='../typings/kue/kue.d.ts' />

import net = require('net');
import redis = require('redis');
import kue = require('kue');

var port: number = 9090;
var server = net.createServer();
var clients = [];

kue.redis.createClient = function() {
    var client = redis.createClient(6379, "localhost");
    return client;
} 

var queue = kue.createQueue();
kue.app.listen(3000);
kue.app.set('title','Orchestra');

server.on('connection', client => {
	clients.push(client);
	
	client.on('data', data => {
		var jsonData : any = JSON.parse(data);
		var bucketId : string = jsonData['table'];
		var job : kue.Job = queue.create(bucketId, jsonData);
		job.ttl(10000);
		job.removeOnComplete(true);
		job.save(err=>{
			if(err) {
				console.log("Error: creating Job");		
			}
		});
		
		job.on('complete', result => {
			client.write(JSON.stringify(result));
		});
		
		job.on('failed attempt', (errorMessage, doneAttempts) => {
			var errorResult : string = "{ \"success\":\"0\", \"message\":"+errorMessage+"}";
			client.write(errorResult);
		});
		
		job.on('failed', errorMessage => {
			var errorResult : string = "{ \"success\":\"0\", \"message\":"+errorMessage+"}";
			client.write(errorResult);
		});
	});
});

server.listen(port);