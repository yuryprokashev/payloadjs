/**
 * Created by py on 04/11/2016.
 */
'use strict';

const EventEmitter = require('events').EventEmitter;
const KafkaAdapter = require('./KafkaAdapter2');
const MonthData = require('./MonthData');
const guid = require('./guid');
const mongoose = require('mongoose');
const MyDates = require('./MyDates');
const KAFKA_TEST = "54.154.211.165";
const KAFKA_PROD = "54.154.226.55";


class PayloadService extends EventEmitter{
    constructor(isProd){
        super();
        var _this = this;
        if(isProd === undefined){
            throw new Error('isProd flag is missing');
        }
        this.serviceName = 'Payload-Service';
        this.kafkaHost = (function(bool){
            let result = bool ? KAFKA_PROD : KAFKA_TEST;
            console.log(result);
            return result;
        })(isProd);
        this.bus = new KafkaAdapter(this.kafkaHost, this.serviceName, 2);
        let requestId = guid();
        this.bus.producer.on('ready', function () {
            _this.bus.subscribe('get-config-response', _this.configure);
            _this.bus.send('get-config-request', {requestId: requestId});
        });

        this.configure = function (msg) {
            let message = JSON.parse(msg.value);
            if(message.requestId === requestId){
                // console.log(message);
                let dbURL = message.responsePayload[0].db.dbURL;
                _this.emit('config-ready', {dbURL: dbURL});
            }
        };
        this.on('config-ready', function (args) {
            mongoose.connect(args.dbURL);
            let schema = require('./payload.schema');
            _this.m = mongoose.model('Payload', schema, 'payloads');
            if(_this.m !== undefined){
                _this.emit('payloads-ready');
            }
            else {
                throw new Error('model creation failed');
            }
            
        });
        this.on('payloads-ready', function () {
            console.log(`${_this.serviceName} bootstrapped`);
            _this.bus.subscribe('message-done', handleMessageDone);
            _this.bus.subscribe('payload-request', handlePayloadRequest);
            _this.bus.subscribe('copy-payload-request', handleCopyPayloadRequest);
            _this.bus.subscribe('clear-payload-request', handleClearPayloadRequest);
            _this.bus.subscribe('get-month-data-request', handleGetMonthDataRequest);

            function logError(err){
                console.log(`Error: ${JSON.stringify(err)}`);
            }

            var init = function (msg, model) {
                var ctx = {
                    originalMsg: {}
                };
                ctx.m = model;
                ctx.msg = msg;
                ctx.occuredAt = MyDates.now();
                return ctx;
            };

            var extractMessage = function (ctx) {
                ctx.originalMsg = JSON.parse(ctx.msg.value);
            };

            var extractPayload = function (ctx) {
                console.log(ctx.originalMsg);
                try {
                    ctx.originalPayload = JSON.parse(ctx.originalMsg.responsePayload.payload);
                }
                catch (err) {
                    return _this.bus.send('payload-response', {error: `error ${err}`});
                }
            };

            var setRequestId = function(ctx) {
                if(ctx.finalPayload === undefined) {
                    ctx.finalPayload = {};
                }
                if(ctx.originalMsg !== null){
                    ctx.finalPayload.requestId = ctx.originalMsg.requestId;
                }
            };

            function handleMessageDone(msg) {

                var map = function(ctx){
                    if(ctx.originalPayload !== undefined) {
                        ctx.finalPayload = {};
                        ctx.finalPayload._id = ctx.originalPayload.id || guid();
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
                    }
                    else {
                        return _this.bus.send('payload-response', {error: `error no original message`});
                    }
                };

                var store = function (ctx, callback) {
                    if(ctx.finalPayload !== undefined) {
                        ctx.finalPayload.storedAt = MyDates.now();
                        var query = {
                            "_id": ctx.finalPayload._id
                        };
                        ctx.m.findOneAndUpdate(query, ctx.finalPayload, {new: true, upsert: true}, function (err, result) {
                            if(err){
                                console.log(err);
                            }
                            else if(result) {
                                callback(result);
                            }
                        });
                    }
                    else {
                        return _this.bus.send('payload-response', {error: `error no original message`});
                    }
                };

                var notify = function (result) {
                    _this.bus.send('payload-done', result);
                };

                // console.log('handleMessage called');
                var ctx = init(msg, _this.m);
                extractMessage(ctx);
                extractPayload(ctx);
                map(ctx);
                store(ctx, notify);
            }

            function handlePayloadRequest(req) {

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
                           _this.bus.send('payload-response', ctx.finalPayload);
                        });
                };

                var ctx = init(req, _this.m);
                extractMessage(ctx);
                constructQuery(ctx);
                setRequestId(ctx);
                get(ctx);
            }

            function handleCopyPayloadRequest(req) {
                // console.log('handleCopyPayloadRequest called');

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
                    // console.log('START COPY');
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
                        newItem.description = item.description;
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
                    _this.bus.send('copy-payload-response',ctx.finalPayload);
                }

                var ctx = init(req, _this.m);
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
            }

            function handleClearPayloadRequest(req){
                // console.log('handleClearPayloadRequest');
                var ctx = init(req, _this.m);
                extractMessage(ctx);
                setRequestId(ctx);
                function replyClear(result){
                    // console.log(`Success ${JSON.stringify(result)}`);
                    ctx.finalPayload.responsePayload = result;
                    ctx.finalPayload.responseErrors = [];
                    _this.bus.send('clear-payload-response',ctx.finalPayload);
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
            }

            function handleGetMonthDataRequest(req){
                // console.log('handleGetMonthDataRequest');
                console.log(req);
                var ctx = init(req, _this.m);
                extractMessage(ctx);
                if(ctx.originalMsg === null){
                    return 0;
                }
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
                                    // console.log(`data aggregated is ${JSON.stringify(data)}`);
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
                    // console.log(`MonthData Success: ${JSON.stringify(result)}`);
                    ctx.finalPayload.responsePayload = result;
                    ctx.finalPayload.responseErrors = [];
                    _this.bus.send('get-month-data-response',ctx.finalPayload);
                }

                aggregateMonthData(ctx)
                    .then(
                        replyMonthData,
                        logError
                    )
            }

        });
    }
}
module.exports = PayloadService;