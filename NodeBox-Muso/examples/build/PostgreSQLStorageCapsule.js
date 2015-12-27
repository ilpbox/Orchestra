/// <reference path="../typings/mysql/mysql.d.ts" />
/// <reference path="NodeOrchestra.ts" />
/// <reference path="../typings/mongo-sql/mongo-sql.d.ts" />
/// <reference path="../typings/pg/pg.d.ts" />
var pg = require('pg');
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
var PostgreSQLStorageCapsule = (function () {
    function PostgreSQLStorageCapsule() {
        var _this = this;
        this.conString = "postgres://username:port@localhost/database";
        this.TABLE_SQL = "select table_name from information_schema.tables;";
        this.builder = require("../node_modules/mongo-sql");
        this.tablenamesToArray = function (result, callback) {
            console.log(JSON.stringify(result));
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
    }
    PostgreSQLStorageCapsule.prototype.handleQuery = function (query, resultHandler, callback) {
        pg.connect(this.conString, function (err, client, done) {
            if (err) {
                done();
                callback(err, null);
                return;
            }
            client.query(query, function (err, result) {
                done();
                if (err) {
                    callback(err, null);
                    return;
                }
                resultHandler(result.rows, callback);
            });
        });
    };
    PostgreSQLStorageCapsule.prototype.executeNativeStatement = function (statement, callback) {
        this.handleQuery(statement, this.dummyCallback, callback);
    };
    return PostgreSQLStorageCapsule;
})();
var sc = new PostgreSQLStorageCapsule();
var fn = new Functions();
var me = new orchestra.Muso(sc, fn);
console.log("Muso:: Started");
