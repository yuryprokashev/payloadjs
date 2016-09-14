/**
 * Created by py on 11/08/16.
 */

var PayloadService;

var MyDates = require('./MyDates');

// param: PayloadModel m - model of payload, that PayloadService will use to store payloads in Mongo DB
// param: Bus b - bus instance to access Kafka
// responsibility: get the Message, parse it and extracts Payload. Saves Payoad in MongoDB
// and then send the message to Kafka
// return: PayloadService api
PayloadService = function (m, b) {

    // param: BusMessage msg
    // function: create a separate context to handle incoming message
    // return: Object context
    var init= function (msg, model) {
        var ctx = {};
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
        ctx.originalPayload = JSON.parse(ctx.originalMsg.payload);
        console.log('EXTRACTED FROM PAYLOAD');
        console.log(ctx.originalPayload);
    };

    var setRequestId = function(ctx) {
        ctx.finalPayload.requestId = ctx.originalMsg.requestId;
    };


    // param: BusMessage msg - message received over Bus.
    // param: PayloadModel m - the Model, where 'payload' will be saved
    // function: handle the incoming 'message-new' Message and send result to Bud, 'payload-new' topic
    // return: void
    var handleMessage = function (msg) {

        console.log('handleMessage called');
        console.log(msg);

        var map = function(ctx){
            ctx.finalPayload = {};
            ctx.finalPayload._id = ctx.originalPayload.id || require('./guid')();
            ctx.finalPayload.type = 1;
            ctx.finalPayload.amount = ctx.originalPayload.amount;
            ctx.finalPayload.dayCode = ctx.originalPayload.dayCode;
            ctx.finalPayload.description = ctx.originalPayload.description || '';
            ctx.finalPayload.labels = ctx.originalPayload.labels;
            ctx.finalPayload.occuredAt = ctx.occuredAt;
            ctx.finalPayload.sourceId = ctx.originalMsg.sourceId;
            ctx.finalPayload.campaignId = ctx.originalMsg.campaignId || 0;
            ctx.finalPayload.userId = ctx.originalMsg.userId;
            ctx.finalPayload.messageId = ctx.originalMsg._id;
            ctx.finalPayload.userToken = ctx.originalMsg.userToken;
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
                    Bus.send('error-new', err);
                    console.log(err);
                }
                else if(result) {
                    callback(result);
                }
            });
        };

        var notify = function (result) {
            b.send('payload-new', result);
        };

        var ctx = init(msg, m);
        extractMessage(ctx);
        extractPayload(ctx);
        map(ctx);
        store(ctx, notify);
    };

    var handlePayloadRequest = function (req) {

        console.log('handlePayloadRequest called');
        console.log(req);

        var constructQuery = function (ctx) {
            ctx.finalPayload = {};
            // ctx.finalPayload.requestToken = ctx.originalPayload.requestToken;
            ctx.query = {};
            ctx.query.type = ctx.originalMsg.payloadType || 1;
            ctx.query.dayCode = ctx.originalMsg.dayCode;
            ctx.query.userId = ctx.originalMsg.user;
            ctx.sortOrder = ctx.originalMsg.sortOrder || {occuredAt: -1};
        };

        var get = function (ctx) {
            // console.log(`constructed { user:  ${ctx.query.user}, type: ${ctx.query.type} }`);
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

    b.subscribe('message-new', handleMessage);
    b.subscribe('payload-request', handlePayloadRequest);
};

module.exports = PayloadService;