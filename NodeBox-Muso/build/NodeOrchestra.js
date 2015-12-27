/// <reference path="../typings/kue/kue.d.ts" />
/// <reference path="../typings/redis/redis.d.ts" />
/// <reference path='../typings/node/node.d.ts' />
//error structure: { "success":"0", "message":"e.message"}
var kue = require('kue');
var redis = require('redis');
var net = require('net');
var NodeOrchestra;
(function (NodeOrchestra) {
    var Maestro = (function () {
        function Maestro(options) {
            this._server = net.createServer();
            this._clients = [];
            this._options = { redisUrl: "localhost", redisPort: 6379, port: 9090 };
            if (options != null) {
                this._options.redisPort = options.redisPort || this._options.redisPort;
                this._options.redisUrl = options.redisUrl || this._options.redisUrl;
                this._options.port = options.port || this._options.port;
            }
            this.initializeServer();
        }
        Maestro.prototype.initializeServer = function () {
            var _this = this;
            console.log("Maestro:: Initializing server");
            kue.redis.createClient = function () {
                var client = redis.createClient(6379, "localhost");
                return client;
            };
            var queue = kue.createQueue();
            this._server.on('connection', function (client) {
                _this._clients.push(client);
                client.on('data', function (data) {
                    try {
                        var jsonData = JSON.parse(data);
                    }
                    catch (e) {
                        var error = { success: 0, message: e };
                        client.write(JSON.stringify(error));
                    }
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
            this._server.listen(this._options.port);
            console.log("Maestro:: Server started");
        };
        return Maestro;
    })();
    NodeOrchestra.Maestro = Maestro;
    var Muso = (function () {
        function Muso(storageCapsule, functions, options) {
            this._maxJobs = 100;
            if (functions != null) {
                this._functions = functions;
            }
            this._options = { redisUrl: "localhost", redisPort: 6379 };
            if (options != null) {
                this._options.redisPort = options.redisPort || this._options.redisPort;
                this._options.redisUrl = options.redisUrl || this._options.redisUrl;
            }
            this._storageCapsule = storageCapsule;
            kue.redis.createClient = function () {
                var client = redis.createClient(6379, "localhost");
                return client;
            };
            this._queue = kue.createQueue();
            this.initializeJobs();
        }
        Muso.prototype.initializeJobs = function () {
            var _this = this;
            console.log("Muso: Initializing Jobs");
            this._storageCapsule.getIdentifiers(function (error, success) {
                if (error == null) {
                    var numberOfJobs = 1;
                    if (success.length < _this._maxJobs) {
                        numberOfJobs = Math.floor(_this._maxJobs / success.length);
                    }
                    success.forEach(function (elem) {
                        _this._queue.process(elem, numberOfJobs, function (job, done) {
                            if (_this._functions != null && job.data["function"] != null) {
                                console.log("Muso: Detected function in query");
                                var fc = job.data["function"];
                                var params = fc["parameter"];
                                var name = fc["name"];
                                var sql = _this._functions[name](params);
                                _this._storageCapsule.executeNativeStatement(sql, done);
                            } /*else if(job.data["$function"] != null) {
                                var fc = job.data["$function"];
                                var
                                this._storageCapsule.executeStatement(job.data, (done, result) => {
                                    result =
                                    done()
                                });
                            }*/
                            else {
                                _this._storageCapsule.executeStatement(job.data, done);
                            }
                            //done(null, result);
                        });
                    });
                }
                else {
                    console.log("Muso: Error occurred while initializing Jobs");
                    console.log(JSON.stringify(error));
                }
            });
        };
        return Muso;
    })();
    NodeOrchestra.Muso = Muso;
})(NodeOrchestra = exports.NodeOrchestra || (exports.NodeOrchestra = {}));
