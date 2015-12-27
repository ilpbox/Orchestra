/// <reference path="../typings/kue/kue.d.ts" />
/// <reference path="../typings/redis/redis.d.ts" />
/// <reference path='../typings/node/node.d.ts' />


//error structure: { "success":"0", "message":"e.message"}
import kue = require('kue');
import redis = require('redis')
import net = require('net');

export module NodeOrchestra {
	export interface IStorageCapasule {
		executeStatement: (statement: any, callback: (err: any, res: any) => void) => void;
		getIdentifiers: (callback: (err: any, res: string[]) => void) => void;
		executeNativeStatement(statement: any, callback: (err: any, res: any) => void): void;
	}
	
	export interface IFunctions{}
	
	interface IOptions {
		redisUrl: string;
		redisPort: number;
		port?: number;		
	}
	
	interface IError {
		success: number;
		message: string;
	}
	
	export class Maestro {
		private _options: IOptions;
		private _server: net.Server = net.createServer();
		private _clients: any[] = [];
		
		private initializeServer(): void {
			console.log("Maestro:: Initializing server");
			kue.redis.createClient = function() {
    			var client = redis.createClient(6379, "localhost");
    			return client;
			} 

			var queue = kue.createQueue();
			
			this._server.on('connection', client => {
				this._clients.push(client);
				
				client.on('data', data => {
					try{
						var jsonData : any = JSON.parse(data);
					} catch(e) {
						var error: IError = {success:0, message:e}
						client.write(JSON.stringify(error))
					}
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
			
			this._server.listen(this._options.port);
			console.log("Maestro:: Server started")
		}
		
		constructor(options?: IOptions) {
			this._options = {redisUrl: "localhost", redisPort:6379, port: 9090};
			if(options != null){
				this._options.redisPort = options.redisPort || this._options.redisPort;
				this._options.redisUrl = options.redisUrl || this._options.redisUrl;
				this._options.port = options.port || this._options.port;
			}
			this.initializeServer();
		}
	}
	
	export class Muso {
		
		private _storageCapsule;
		private _queue: kue.Queue;
		private _maxJobs: number = 100;
		private _functions: IFunctions;
		private _options: IOptions;
		
		private initializeJobs(): void {
			console.log("Muso: Initializing Jobs");
			this._storageCapsule.getIdentifiers((error: any, success: any) => {
					if(error == null) {
						var numberOfJobs:number = 1;
						if(success.length<this._maxJobs) {
							numberOfJobs = Math.floor(this._maxJobs/success.length);
						}
						success.forEach(elem => {
							this._queue.process(elem,numberOfJobs, (job: kue.Job, done) => {
								if(this._functions != null && job.data["function"] != null) {
									console.log("Muso: Detected function in query")
									var fc = job.data["function"];
									var params = fc["parameter"];
									var name = fc["name"];
									var sql = this._functions[name](params);
									this._storageCapsule.executeNativeStatement(sql, done);
								} /*else if(job.data["$function"] != null) {
									var fc = job.data["$function"];
									var 
									this._storageCapsule.executeStatement(job.data, (done, result) => {
										result = 
										done()
									});	
								}*/ else {
									this._storageCapsule.executeStatement(job.data, done);
								}
								//done(null, result);
							});
						});
					} else {
						console.log("Muso: Error occurred while initializing Jobs");
                        console.log(JSON.stringify(error));
					}
				}	
			);
		}
		
		constructor(storageCapsule: IStorageCapasule, functions?: IFunctions, options?: IOptions){
			if(functions != null) {
				this._functions = functions;
			}
			this._options = {redisUrl: "localhost", redisPort:6379};
			if(options != null) {
				this._options.redisPort = options.redisPort || this._options.redisPort;
				this._options.redisUrl = options.redisUrl || this._options.redisUrl;
			}
			this._storageCapsule = storageCapsule;
			kue.redis.createClient = function() {
   	 			var client = redis.createClient(6379, "localhost");
    			return client;
			} 
			this._queue = kue.createQueue();
			
			this.initializeJobs();
		}
	}
}