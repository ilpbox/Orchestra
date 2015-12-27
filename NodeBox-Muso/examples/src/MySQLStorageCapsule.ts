/// <reference path="../typings/mysql/mysql.d.ts" />
/// <reference path="NodeOrchestra.ts" />
/// <reference path="../typings/mongo-sql/mongo-sql.d.ts" />

import mysql = require('mysql');
import orchestra = require('NodeOrchestra');

class Functions implements orchestra.IFunctions {
	public getSimilar(params: string[]) {
		var sql: string = "SELECT * FROM "+params[0]+" USING DISTANCE MINKOWSKI(2)(\'"+params[1]+ "\', "+params[2]+") ORDER USING DISTANCE LIMIT " + params[3];
		return sql;
	}
	
	public getSimilarByShotId(params: string[]): string {
		var sql: string  = "WITH q AS (SELECT "+params[0]+" FROM "+params[1]+" WHERE shotid = " + params[2] + ") SELECT shotid FROM "+params[1]+", q USING DISTANCE MINKOWSKI(2)(q."+params[0]+", "+params[1]+"."+params[0]+") ORDER USING DISTANCE LIMIT " + params[3];
		return sql;
	}
	
	public processShot(params: string[]): string {
		var sql: string = "SELECT * FROM "+params[0]+" WHERE shotid = " +params[1];
		return sql;
	}
	
	public selectFrom(params: string[]): string {
		var sql: string = "SELECT * FROM "+params[0]+";"
		return sql;
	}
}

class MySQLStorageCapsule implements orchestra.IStorageCapasule {
	private pool: mysql.IPool;
	private TABLE_SQL: string = "select table_name from information_schema.tables;"; 
	private builder = <mongosql>require("../node_modules/mongo-sql")
	
	constructor() {
		this.pool = mysql.createPool({
			connectionLimit: 100,
			host: 'localhost',
			user     : 'username',
			password: 'password',
			database: 'database',
			debug: false
		});
	}
	
	private handleQuery(query: string, resultHandler: (result: any, callback: (err: any, result: any) => void) => void, callback: (err: any, result: any) => void) : void {
		this.pool.getConnection((err, connection) => {
			if(err) {
				connection.release();
				callback(err, null);
				return;
			}
			query = query.replace(/\"/g,'`');
			connection.query(query, (err, rows) => {
				if(err) {
					callback(err, null);
				} else {
					resultHandler(rows, callback);
				}
				connection.release();
				return;
			});
		});
	}
	
	public executeNativeStatement(statement: any, callback: (err: any, res: any) => void): void {
		this.handleQuery(statement, this.dummyCallback, callback);
	}
	
	private tablenamesToArray = (result: any[], callback: (err: any, res: any) => void) => {
		var returnArray: string[];
		returnArray = result.map((value: any)=>{
			return value["table_name"]
		});
		callback(null, returnArray);
	}
	
	private dummyCallback = (result: any[], callback: (err: any, res: any) => void) => {
		callback(null, result);
	}
	
	public executeStatement = (statement: any, callback: (err: any, res: any) => void) => {
		var sql = this.builder.sql(statement);
		console.log(sql.toString());
		this.handleQuery(sql.toString(), this.dummyCallback, callback);
	};
	
	public getIdentifiers = (callback: (err: any, res: string[]) => void) => {
		this.handleQuery(this.TABLE_SQL, this.tablenamesToArray, callback);
	}
}

var sc: orchestra.IStorageCapasule = new MySQLStorageCapsule();
var fn: orchestra.IFunctions = new Functions();


var me = new orchestra.Muso(sc,fn);
console.log("Muso:: Started");