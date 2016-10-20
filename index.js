#!/usr/bin/env node
var debug = require('debug')('infinidb-management-tool-master:infinidb-management-tool-master');
var fs = require('fs');
var queryExecutor = require('node-database-executor');
var defaultDBConfig = {
	type: "database",
	databaseType: "mysql",
	engine: "infinidb",
	database: "",
	host: "",
	port: "",
	user: "",
	password: "",
	connectionLimit: 0
};
var argv = require('yargs').argv;
console.log(argv.host);
console.log(argv.db);
console.log(argv.tmpdir);

if (argv.host && argv.db && argv.user && argv.pass && argv.tmpdir && argv.expdir && argv.type) {
    console.log('Good to go!');
} 
else {
    console.log('You shall not pass!');
}

var dirTempo = argv.tmpdir;

var createStreamSQL = fs.createWriteStream("F:/filemanagement/tempo/DBBackupScript.sql", {'flags': 'a'});
var createStreamTXT = fs.createWriteStream("F:/filemanagement/tempo/TableMapping.txt");
var varShowCreateTable = "";
if (!fs.existsSync(dirTempo)){
	console.log('Directory already exists! Please delete it manually.');
	process.exit(0);
}
else 
	// create directory
	// fs.mkdirSync(dir);
	var dir = 'F:/filemanagement/tempo/';
	console.log(dir);
	if (!fs.existsSync(dir)) {
		console.log('Folder does not exists!');
		return;			
	} else {
       	// create directory
		// fs.mkdirSync(exports.folderPath + 'F:/filemanagement/tempo/');
				
		//createStreamSQL.write("use axiomdemo;\n");
		//"use argv.db;"
		createStreamSQL.write("use " + argv.db + ";\n"); 
		
		createStreamSQL.write("show tables;\n");
		//"show tables;"
		getTables(argv.db, function(resultTables){
			debug("After getting table(s) " + JSON.stringify(resultTables));
			//process.exit(0);
		});
		
		createStreamTXT.write("tablename           |           filename\r\n");
		createStreamTXT.write("temp");
		
		// createStream.write("show create table temp;\n");
		// createStreamSQL.write("drop table if exists temp;\n");
		getShowCreateTable('temp', function(resultCreateTable){ 
			debug("After getting show create table " + JSON.stringify(resultCreateTable));
			debug(resultCreateTable.content[0]['Table']);
			debug(JSON.stringify(resultCreateTable.content[0]['Create Table']));
			varShowCreateTable += JSON.stringify(resultCreateTable.content[0]['Create Table']) + ";\n";
			console.log(varShowCreateTable);

			createStreamSQL.write("drop table if exists temp;\n");
			
			createStreamSQL.write(resultCreateTable.content[0]['Create Table'] + ";\n");
			
		});

		getOutFile(argv.db, function(resultOutFile){
			createStreamSQL.write(resultOutFile + ";\n");
			
		});
		
		//toEnd();
		/*createStreamSQL.end();
		createStreamTXT.end();*/
		
		console.log('Inside');
    }

    function getShowCreateTable(temp,cbFunc) { 
		// "show create table temp", 
		var query = "show create table " + temp; 
		var requestData = {};
		requestData.query = query;
		requestData.dbConfig = defaultDBConfig;
		//debug(requestData.dbConfig);
		queryExecutor.executeRawQuery(requestData, function(data) {
            if (data.status == true) {
		        cbFunc({
		        	status: true,
		            content: data.content
		        });
		    } else {
		        cbFunc({
		            status: false,
		            error: data.error
		        });
		    }
        });
	}
	function getTables(db,cbFunc){
		var query = "show tables"; 	
		var requestData = {};
		requestData.query = query;
		debug(requestData.query);
		requestData.dbConfig = defaultDBConfig;	
		queryExecutor.executeRawQuery(requestData, function(data) {
            if (data.status == true) {
		        cbFunc({
		        	status: true,
		            content: data.content
		        });
		    } else {
		        cbFunc({
		            status: false,
		            error: data.error
		        });
		    }
        });	
	}
	function getOutFile(db,cbFunc){
		var query = "select * from temp" + " into OUTFILE 'F:/filemanagement/BackupTempo/Datatempo.csv'" + " fields terminated by ',' enclosed by '\"' lines terminated by '\r\n' ";
		debug(query);
		var requestData = {
            "query": query,
            "dbConfig": defaultDBConfig
        };           
		queryExecutor.executeRawQuery(requestData, function(data) {
			debug("After getting OUTFILE " +  JSON.stringify(data));
		    if (data.status == true) {
		        cbFunc({
		        	status: true,
		        	content: data.content
		        });
		    } else {
		        cbFunc({
		            status: false,
		            error: data.error
		        });
		    }
        });	

	}	
	function toEnd(){
		createStreamSQL.end();
		createStreamTXT.end();	
	}

	// tablename | filename
	// loop
	// 	show create table
	// 		drop table if exists && create table script >> create_table.sql

// ./index.js --host="192.168.1.1" --db="192.168.1.1" --user="192.168.1.1" --pass="192.168.1.1" --tmpdir="192.168.1.1" --expdir="192.168.1.1" --type="192.168.1.1" 