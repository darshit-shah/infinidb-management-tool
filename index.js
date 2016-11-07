#!/usr/bin/env node

var debug = require('debug')('infinidb-management-tool:infinidb-management-tool');
var fs = require('fs');
var queryExecutor = require('node-database-executor');
var argv = require('yargs').argv;
debug(argv);

if (argv.action && argv.engine && argv.port && argv.host && argv.db && argv.user && argv.pass && argv.tmpdir) {
  console.log('Good to go!');
} else {
  console.log('You shall not pass!', argv.action, argv.engine, argv.port, argv.host, argv.db, argv.user, argv.pass, argv.tmpdir);
  process.exit(0);
}

var defaultDBConfig = {
  type: "database",
  databaseType: "mysql",
  engine: argv.engine,
  database: argv.db,
  host: argv.host,
  port: argv.port,
  user: argv.user,
  password: argv.pass,
  connectionLimit: 0,
  acquireTimeout: 5 * 60 * 1000
};


var backupDirectory = argv.tmpdir;
// backupDirectory = "F:/filemanagement/tempo";

if (argv.action.toString() == "restore") {
  dropRecreateTables(defaultDBConfig.database, function() {
    process.exit(0);
  });
} else if (argv.action.toString() == "backup") {
  var createStreamSQL = fs.createWriteStream(backupDirectory + "/DBBackupScript.sql", { 'flags': 'w' });
  var createStreamTXT = fs.createWriteStream(backupDirectory + "/TableMapping.txt");
  var varShowCreateTable = "";
  if (fs.existsSync(backupDirectory)) {
    console.log('Directory already exists! Please delete it manually.');
    // process.exit(0);
  }
  // create directory
  // fs.mkdirSync(backupDirectory);

  createStreamTXT.write("tableName,fileName\r\n");
  getTables(argv.db, function(resultTables) {
    if (resultTables.status === false) {
      console.log(resultTables);
      return;
    }

    function processTables(index) {
      if (index >= resultTables.content.length) {
        createStreamSQL.end();
        createStreamSQL.destroySoon();
        createStreamTXT.end();
        createStreamTXT.destroySoon();
        process.exit(0);
        return;
      }
      // var currTableName = resultTables.content[index]["Tables_in_" + defaultDBConfig.database];
      var currTableName = resultTables.content[index]["TABLE_NAME"];
      debug((index + 1) + " out of " + resultTables.content.length + ". Processing Table " + currTableName);
      getShowCreateTable(currTableName, function(tableData) {
        if (tableData.status === false) {
          console.log(tableData);
          return;
        }
        createStreamSQL.write("drop table if exists `" + currTableName + "`;\r\n" + tableData.content[0]['Create Table'] + ";\r\n\r\n");
        getOutFile(currTableName, function(queryResult) {
          if (queryResult.status === false) {
            console.log(queryResult);
            return;
          }
          setTimeout(function() {
            processTables(index + 1);
          }, 100);
        });
      });
    }
    processTables(0);
  });
} else {
  console.log("Unknown action " + argv.action);
  process.exit(0);
}

function getTables(dbName, cb) {
  //var query = "show tables;";
  var query = "select TABLE_NAME from TABLES where TABLE_SCHEMA='"+dbName+"' and TABLE_TYPE='BASE TABLE';";
  var requestData = {};
  requestData.query = query;
  debug(requestData.query);
  requestData.dbConfig = defaultDBConfig;
  queryExecutor.executeRawQuery(requestData, cb);
}

function getShowCreateTable(tableName, cb) {
  var query = "show create table `" + tableName + '`;';
  var requestData = {};
  requestData.query = query;
  requestData.dbConfig = defaultDBConfig;
  queryExecutor.executeRawQuery(requestData, cb);
}

function getOutFile(tableName, cb) {
  createStreamTXT.write(tableName + "," + tableName + "\r\n");
  var query = "select * from `" + tableName + "` into OUTFILE '" + backupDirectory + "/" + tableName + ".csv'" + " fields terminated by ',' enclosed by '\"' lines terminated by '\r\n';";
  debug(query);
  var requestData = {
    "query": query,
    "dbConfig": defaultDBConfig
  };
  queryExecutor.executeRawQuery(requestData, cb);
}

function dropRecreateTables(dbName, cb) {
  fs.readFile(backupDirectory + "/DBBackupScript.sql", function(err, data) {
    var requestData = {
      "query": "use " + dbName + ";" + data,
      "dbConfig": defaultDBConfig
    };
    queryExecutor.executeRawQuery(requestData, function(result){
      debug(result);
    });
  });
}

// tablename | filename
// loop
//  show create table
//    drop table if exists && create table script >> create_table.sql

/*
node index.js --action="backup" --engine="infinidb" --port="3307" --host="localhost" --db="axiomdemo" --user="usr" --pass="usr" --tmpdir="/tmp/database_backup" 
*/
