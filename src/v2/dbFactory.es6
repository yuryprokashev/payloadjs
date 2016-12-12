/**
 *Created by py on 14/11/2016
 */
'use strict';
const Mongoose = require('mongoose');

module.exports = dbURL => {
    let connection = Mongoose.connect(dbURL);
    Mongoose.connection.on('connected',()=>{
        console.log('connected to db');
    });
    Mongoose.connection.on('error', ()=>{
        console.log(`mongoose failed to connect to ${dbURL}`);
    });
    Mongoose.connection.on('disconnected', ()=>{
        console.log('disconnected from db');
    });
    process.on('SIGINT', ()=>{
        Mongoose.connection.close(()=>{
            console.log('close db connection due to node app exit');
            process.exit(0);
        });
    });
    return connection;
};