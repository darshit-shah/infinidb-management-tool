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

if (argv.action.toString() == "restore") {
  dropRecreateTables(defaultDBConfig.database, function () {
    fs.readFile(backupDirectory + "/TableMapping.txt", function (err, data) {
      data = data.toString();
      var lines = data.split("\r\n");
      lines = lines.map(function (l) {
        return l.split(",");
      });
      function processTables(index) {
        if (index >= lines.length) {
          process.exit(0);
          return;
        }
        debug("Processing lines ", index + 1, lines[index]);
        if (lines[index][0].length == 0) {
          processTables(index + 1);
        }
        loadFile(lines[index][0], function (queryResult) {
          if (queryResult.status === false) {
            debug(queryResult);
            return;
          }
          processTables(index + 1);
        });
      }
      processTables(1);
    });
  });
} else if (argv.action.toString() == "backup") {

  var createStreamSQL = fs.createWriteStream(backupDirectory + "/DBBackupScript.sql", { 'flags': 'w' });
  var createStreamTXT = fs.createWriteStream(backupDirectory + "/TableMapping.txt");
  var createStreamSkippedInCreate = fs.createWriteStream(backupDirectory + "/SkippedTablesInCreate.txt");
  var createStreamSkippedInOut = fs.createWriteStream(backupDirectory + "/SkippedTablesInOutput.txt");

  var varShowCreateTable = "";
  if (fs.existsSync(backupDirectory)) {
    debug('Directory already exists! Please delete it manually.');
  }
  /*
  //create directory
  fs.mkdirSync(backupDirectory);
  */

  createStreamTXT.write("tableName,fileName\r\n");
  createStreamSkippedInCreate.write("tableName\r\n");
  createStreamSkippedInOut.write("tableName\r\n");

  getTables(argv.db, function (resultTables) {
    if (resultTables.status === false) {
      debug(resultTables);
      return;
    }
    console.log("Number of tables: ", resultTables.content.length);

    function processTables(index) {
      if (index >= resultTables.content.length) {
        createStreamSQL.end();
        createStreamSQL.destroySoon();
        createStreamTXT.end();
        createStreamTXT.destroySoon();
        process.exit(0);
        return;
      }


      var currTableName = resultTables.content[index]["TABLE_NAME"];
      debug((index + 1) + " out of " + resultTables.content.length + ". Processing Table " + currTableName);
      getShowCreateTable(currTableName, function (tableData) {

        if (tableData.status === false) {
          debug(tableData);
          console.log("error in getting createTable script");
          createStreamSkippedInCreate.write(currTableName + "\r\n");
          
          setTimeout(function () {
            processTables(index + 1);
          }, 100);
          // return;
        }
        else {
          createStreamSQL.write("drop table if exists `" + currTableName + "`;\r\n" + tableData.content[0]['Create Table'] + ";\r\n\r\n");
          getOutFile(currTableName, function (queryResult) {
            if (queryResult.status === false) {
              debug(queryResult);
              console.log("error in creating output csv file");
              createStreamSkippedInOut.write(currTableName + "\r\n");
              // return;
            }

            setTimeout(function () {
              processTables(index + 1);
            }, 100);
          });
        }
      });
    }
    processTables(0);
  });
} else {
  debug("Unknown action " + argv.action);
  process.exit(0);
}

function getTables(dbName, cb) {
  var query = "select TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA='" + dbName + "' and TABLE_TYPE='BASE TABLE';";
  var requestData = {};
  requestData.query = query;
  debug(requestData.query);
  requestData.dbConfig = defaultDBConfig;
  queryExecutor.executeRawQuery(requestData, function (queryResponse) {
    // console.log("query Result",queryResponse);
    cb(queryResponse);
  });
}

function getShowCreateTable(tableName, cb) {
  console.log("step1:: getting create Table schema for "+ tableName);
  var query = "show create table `" + tableName + '`;';
  var requestData = {};
  requestData.query = query;
  requestData.dbConfig = defaultDBConfig;
  queryExecutor.executeRawQuery(requestData, function (queryResponse) {
    // console.log("queryResponse", queryResponse);
    cb(queryResponse);
  });
}

function getOutFile(tableName, cb) {
  console.log("step2:: creating Output csv File for "+tableName + "\n");
  createStreamTXT.write(tableName + "," + tableName + "\r\n");
  var query = "select * from `" + tableName + "` into OUTFILE '" + backupDirectory + "/" + tableName + ".csv'" + " fields terminated by ',' enclosed by '\"' lines terminated by '\r\n';";
  debug(query);
  var requestData = {
    "query": query,
    "dbConfig": defaultDBConfig
  };
  queryExecutor.executeRawQuery(requestData, function (queryResponse) {
    // console.log("getOutFileQueryRes",queryResponse);
    cb(queryResponse);
  });
}

function loadFile(tableName, cb) {
  var query = "LOAD DATA INFILE '" + backupDirectory + "/" + tableName + ".csv' INTO TABLE `" + tableName + "`  fields terminated by ',' enclosed by '\"' lines terminated by '\r\n';";
  debug(query);
  var requestData = {
    "query": query,
    "dbConfig": defaultDBConfig
  };
  queryExecutor.executeRawQuery(requestData, cb);
}

function dropRecreateTables(dbName, cb) {
  fs.readFile(backupDirectory + "/DBBackupScript.sql", function (err, data) {
    var requestData = {
      "query": "use " + dbName + ";" + data,
      "dbConfig": defaultDBConfig
    };
    queryExecutor.executeRawQuery(requestData, function (result) {
      debug("dropRecreateTables", result);
      cb();
    });
  });
}



/*
 * node index.js --action="backup" --engine="infinidb" --port="3307" --host="localhost" --db="axiomdemo" --user="usr" --pass="usr" --tmpdir="/tmp/tmpdatabase_backup"
 * */

