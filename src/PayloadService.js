/**
 * Created by py on 11/08/16.
 */

"use strict";

var PayloadService;

var MyDates = require('./MyDates');
const MonthData = require('./MonthData');


// param: PayloadModel m - model of payload, that PayloadService will use to store payloads in Mongo DB
// param: Bus b - bus instance to access Kafka
// responsibility: get the Message, parse it and extracts Payload. Saves Payoad in MongoDB
// and then send the message to Kafka
// return: PayloadService api
PayloadService = function (m, b) {

    function logError(err){
        console.log(`Error: ${JSON.stringify(err)}`);
    }

    // param: BusMessage msg
    // function: create a separate context to handle incoming message
    // return: Object context
    var init= function (msg, model) {
        var ctx = {
            originalMsg: {}
        };
        ctx.m = model;
        ctx.msg = msg;
        ctx.occuredAt = MyDates.now();
        return ctx;
    };

    var extractMessage = function (ctx) {
        console.log(ctx.msg);
        ctx.originalMsg = JSON.parse(ctx.msg.value);
        console.log('EXTRACTED FROM MESSAGE');
        console.log(ctx.originalMsg);
    };

    var extractPayload = function (ctx) {
        ctx.originalPayload = JSON.parse(ctx.originalMsg.responsePayload.payload);
        // ctx.originalPayload = ctx.originalMsg.responsePayload.payload;

        console.log('EXTRACTED FROM PAYLOAD');
        console.log(ctx.originalPayload);
    };

    var setRequestId = function(ctx) {
        if(ctx.finalPayload === undefined) {
            ctx.finalPayload = {};
        }
        ctx.finalPayload.requestId = ctx.originalMsg.requestId;
    };


    // param: BusMessage msg - message received over Bus.
    // param: PayloadModel m - the Model, where 'payload' will be saved
    // function: handle the incoming 'message-done' Message and send result to Bus, 'payload-done' topic
    // return: void
    var handleMessageDone = function (msg) {

        var map = function(ctx){
            ctx.finalPayload = {};
            ctx.finalPayload._id = ctx.originalPayload.id || require('./guid')();
            ctx.finalPayload.type = 1;
            ctx.finalPayload.amount = ctx.originalPayload.amount;
            ctx.finalPayload.dayCode = ctx.originalPayload.dayCode;
            ctx.finalPayload.monthCode = ctx.originalPayload.monthCode;
            ctx.finalPayload.description = ctx.originalPayload.description || '';
            ctx.finalPayload.labels = ctx.originalPayload.labels;
            ctx.finalPayload.occuredAt = ctx.occuredAt;
            ctx.finalPayload.sourceId = ctx.originalMsg.responsePayload.sourceId;
            ctx.finalPayload.campaignId = ctx.originalMsg.responsePayload.campaignId || 0;
            ctx.finalPayload.userId = ctx.originalMsg.responsePayload.userId;
            ctx.finalPayload.messageId = ctx.originalMsg.responsePayload._id;
            ctx.finalPayload.userToken = ctx.originalMsg.responsePayload.userToken;
            ctx.finalPayload.commandId = ctx.originalMsg.responsePayload.commandId;
            console.log('MAPPED');
            console.log(ctx.finalPayload);
        };

        var store = function (ctx, callback) {
            ctx.finalPayload.storedAt = MyDates.now();
            var query = {
                "_id": ctx.finalPayload._id
            };
            ctx.m.findOneAndUpdate(query, ctx.finalPayload, {new: true, upsert: true}, function (err, result) {
                if(err){
                    b.send('error-new', err);
                    console.log(err);
                }
                else if(result) {
                    callback(result);
                }
            });
        };

        var notify = function (result) {
            b.send('payload-done', result);
        };

        console.log('handleMessage called');
        var ctx = init(msg, m);
        extractMessage(ctx);
        extractPayload(ctx);
        map(ctx);
        store(ctx, notify);
    };

    var handlePayloadRequest = function (req) {

        console.log('handlePayloadRequest called');
        console.log(req);

        function isGetDay(originalMessage) {
            return originalMessage.dayCode !== undefined;
        }
        
        function isGetMonth(originalMessage) {
            return originalMessage.monthCode !== undefined;
        }

        var constructQuery = function (ctx) {
            ctx.finalPayload = {};
            // ctx.finalPayload.requestToken = ctx.originalPayload.requestToken;
            ctx.query = {};
            ctx.query.type = ctx.originalMsg.payloadType || 1;
            if(isGetDay(ctx.originalMsg)){
                ctx.query.dayCode = ctx.originalMsg.dayCode;
            }
            if(isGetMonth(ctx.originalMsg)){
                ctx.query.monthCode = ctx.originalMsg.monthCode;
            }

            ctx.query.userId = ctx.originalMsg.user;
            ctx.sortOrder = ctx.originalMsg.sortOrder || {occuredAt: -1};
        };

        var get = function (ctx) {
            // console.log(`constructed { user:  ${ctx.query.user}, type: ${ctx.query.type} }`);
            ctx.query['labels.isDeleted'] = false;
            // ctx.query.labels = {isDeleted: false};
            // console.log(`GET query is ${JSON.stringify(ctx.query)}`);
            ctx.m.find(ctx.query)
                .sort(ctx.sortOrder)
                .exec(function (err, data) {
                    if(err){
                        ctx.finalPayload = {error: err};
                        return err;
                    }
                    ctx.finalPayload.payload = data;
                    // console.log(data);
                    b.send('payload-response', ctx.finalPayload);
                });
        };

        var ctx = init(req, m);
        extractMessage(ctx);
        constructQuery(ctx);
        setRequestId(ctx);
        get(ctx);
    };

    var handleCopyPayloadRequest = function(req) {
        console.log('handleCopyPayloadRequest called');
        
        function constructQueryAndFindData(ctx){
            ctx.query = {};
            ctx.query.type = ctx.originalMsg.payloadType || 1;
            ctx.query.monthCode = ctx.originalMsg.sourcePeriod;
            ctx.query.userId = ctx.originalMsg.user;
            ctx.query['labels.isDeleted'] = false;

            ctx.foundPayloads = [];

            return new Promise(function(resolve, reject){
                ctx.m.find(ctx.query)
                    .exec(
                        function (err, data) {
                            if(err){reject(err);}
                            resolve(data);
                        }
                    );
            });

        }
        function copy(data) {
            console.log('START COPY');
            function newDayCode(oldDayCode, targetMonth){
                if(oldDayCode.length === 8){
                    let day = oldDayCode.substring(6,8);
                    return `${targetMonth}${day}`;
                }
                else {
                    throw new Error('oldDayCode has wrong length(8 chars expected)');
                }

            }
            function createPayloadCopy(item){
                let newItem = {};
                newItem._id = require('./guid')();
                newItem.type = item.type;
                newItem.amount = item.amount;
                newItem.dayCode = newDayCode(item.dayCode, ctx.originalMsg.targetPeriod);
                newItem.monthCode = ctx.originalMsg.targetPeriod;
                newItem.description = `copy ${item.description}`;
                newItem.labels = {isPlan:true, isDeleted: false};
                newItem.occuredAt = ctx.originalMsg.occuredAt;
                newItem.storedAt = MyDates.now();
                newItem.sourceId = 2; // 2 for Api pfin
                newItem.campaignId = null;
                newItem.userId = item.userId;
                newItem.messageId = null;
                newItem.userToken = null;
                newItem.commandId = ctx.originalMsg.commandId;
                return ctx.m.create(newItem);
            }
            let newData = data.map(createPayloadCopy);
            return Promise.all(newData);
        }
        function reply(result){
            ctx.finalPayload.responsePayload = result;
            ctx.finalPayload.responseErrors = [];
            b.send('copy-payload-response',ctx.finalPayload);
        }

        var ctx = init(req, m);
        extractMessage(ctx);
        setRequestId(ctx);
        constructQueryAndFindData(ctx)
            .then(
                copy,
                logError
            )
            .then(
                reply,
                logError
            );
    };

    var handleClearPayloadRequest = function(req){
        console.log('handleClearPayloadRequest');
        var ctx = init(req, m);
        extractMessage(ctx);
        setRequestId(ctx);
        function replyClear(result){
            console.log(`Success ${JSON.stringify(result)}`);
            ctx.finalPayload.responsePayload = result;
            ctx.finalPayload.responseErrors = [];
            b.send('clear-payload-response',ctx.finalPayload);
        }
        function findTargetMonthData(ctx){
            ctx.query = {};
            ctx.query.type = ctx.originalMsg.payloadType || 1;
            ctx.query.monthCode = ctx.originalMsg.targetPeriod;
            ctx.query.userId = ctx.originalMsg.user;

            return new Promise(function(resolve, reject){
                ctx.m.update(ctx.query, {"labels.isDeleted": true}, {multi:true})
                    .exec(
                        function(err, data){
                            if(err){reject(err);}
                            resolve(data);
                        }
                    );
            });
        }

        findTargetMonthData(ctx)
            .then(
                replyClear,
                logError
            )
    };
    
    var handleGetMonthDataRequest = function(req){
        console.log('handleGetMonthDataRequest');
        var ctx = init(req, m);
        extractMessage(ctx);
        setRequestId(ctx);

        function aggregateMonthData(ctx) {
            let q = ctx.originalMsg;
            ctx.query = [
                {$match: {userId: q.user, "labels.isDeleted": false}},
                {$project: {_id:1, amount:1, monthCode: 1, isPlanned: {$cond:{if:{$eq:["$labels.isPlan",true]}, then:"plan", else:"fact"}}}},
                {$match: {monthCode: q.targetPeriod}},
                {$project: { _id:1, amount:1,isPlanned: "$isPlanned"}},
                {$group: {_id: "$isPlanned", total: {$sum: "$amount"}}}
            ];

            return new Promise(function (resolve, reject){
                ctx.m.aggregate(ctx.query)
                    .exec(
                        function(err, data){
                            if(err){
                                console.log(err);
                                reject(err);
                            }
                            console.log(`data aggregated is ${JSON.stringify(data)}`);
                            var monthData;
                            function findPlan(item) {
                                return item._id === 'plan';
                            }

                            function findFact(item) {
                                return item._id === 'fact';
                            }
                            var fact = data.find(findFact) || {_id: 'fact', total: 0};
                            var plan = data.find(findPlan) || {_id: 'plan', total: 0};
                            monthData = new MonthData(fact.total, plan.total);

                            resolve(monthData);
                        }
                    );
            });
        }
        
        function replyMonthData(result){
            console.log(`MonthData Success: ${JSON.stringify(result)}`);
            ctx.finalPayload.responsePayload = result;
            ctx.finalPayload.responseErrors = [];
            b.send('get-month-data-response',ctx.finalPayload);
        }

        aggregateMonthData(ctx)
            .then(
                replyMonthData,
                logError
            )
    };

    b.subscribe('message-done', handleMessageDone);
    b.subscribe('payload-request', handlePayloadRequest);
    b.subscribe('copy-payload-request', handleCopyPayloadRequest);
    b.subscribe('clear-payload-request', handleClearPayloadRequest);
    b.subscribe('get-month-data-request', handleGetMonthDataRequest);
};

module.exports = PayloadService;