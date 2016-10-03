#!/usr/bin/env node
var argv = require('yargs').argv;

if (argv.host && argv.db && argv.user && argv.pass && argv.tmpdir && argv.expdir && argv.type) {
    console.log('Good to go!');
} else {
    console.log('You shall not pass!');
}

if directory exists
	error
	exit
else
	create directory
	"use argv.db;"
	"show tables;"
	create file mapping.txt
	// tablename | filename
	loop
		show create table
			drop table if exists && create table script >> create_table.sql



// ./index.js --host="192.168.1.1" --db="192.168.1.1" --user="192.168.1.1" --pass="192.168.1.1" --tmpdir="192.168.1.1" --expdir="192.168.1.1" --type="192.168.1.1" 