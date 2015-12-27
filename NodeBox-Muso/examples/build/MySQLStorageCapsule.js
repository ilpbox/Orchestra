/// <reference path="../typings/mysql/mysql.d.ts" />
/// <reference path="NodeOrchestra.ts" />
/// <reference path="../typings/mongo-sql/mongo-sql.d.ts" />
var mysql = require('mysql');
var orchestra = require('NodeOrchestra');
var Functions = (function () {
    function Functions() {
    }
    Functions.prototype.getSimilar = function (params) {
        var sql = "SELECT * FROM " + params[0] + " USING DISTANCE MINKOWSKI(2)(\'" + params[1] + "\', " + params[2] + ") ORDER USING DISTANCE LIMIT " + params[3];
        return sql;
    };
    Functions.prototype.getSimilarByShotId = function (params) {
        var sql = "WITH q AS (SELECT " + params[0] + " FROM " + params[1] + " WHERE shotid = " + params[2] + ") SELECT shotid FROM " + params[1] + ", q USING DISTANCE MINKOWSKI(2)(q." + params[0] + ", " + params[1] + "." + params[0] + ") ORDER USING DISTANCE LIMIT " + params[3];
        return sql;
    };
    Functions.prototype.processShot = function (params) {
        var sql = "SELECT * FROM " + params[0] + " WHERE shotid = " + params[1];
        return sql;
    };
    Functions.prototype.selectFrom = function (params) {
        var sql = "SELECT * FROM " + params[0] + ";";
        return sql;
    };
    return Functions;
})();
var MySQLStorageCapsule = (function () {
    function MySQLStorageCapsule() {
        var _this = this;
        this.TABLE_SQL = "select table_name from information_schema.tables;";
        this.builder = require("../node_modules/mongo-sql");
        this.tablenamesToArray = function (result, callback) {
            var returnArray;
            returnArray = result.map(function (value) {
                return value["table_name"];
            });
            callback(null, returnArray);
        };
        this.dummyCallback = function (result, callback) {
            callback(null, result);
        };
        this.executeStatement = function (statement, callback) {
            var sql = _this.builder.sql(statement);
            console.log(sql.toString());
            _this.handleQuery(sql.toString(), _this.dummyCallback, callback);
        };
        this.getIdentifiers = function (callback) {
            _this.handleQuery(_this.TABLE_SQL, _this.tablenamesToArray, callback);
        };
        this.pool = mysql.createPool({
            connectionLimit: 100,
            host: 'localhost',
            user: 'username',
            password: 'password',
            database: 'database',
            debug: false
        });
    }
    MySQLStorageCapsule.prototype.handleQuery = function (query, resultHandler, callback) {
        this.pool.getConnection(function (err, connection) {
            if (err) {
                connection.release();
                callback(err, null);
                return;
            }
            query = query.replace(/\"/g, '`');
            connection.query(query, function (err, rows) {
                if (err) {
                    callback(err, null);
                }
                else {
                    resultHandler(rows, callback);
                }
                connection.release();
                return;
            });
        });
    };
    MySQLStorageCapsule.prototype.executeNativeStatement = function (statement, callback) {
        this.handleQuery(statement, this.dummyCallback, callback);
    };
    return MySQLStorageCapsule;
})();
var sc = new MySQLStorageCapsule();
var fn = new Functions();
var me = new orchestra.Muso(sc, fn);
console.log("Muso:: Started");
