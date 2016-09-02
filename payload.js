/**
 * Created by py on 11/08/16.
 */

var PayloadService;

var PayloadModel = require('./PayloadModel');
var Bus = require('./BusService');
var MyDates = require('./MyDates');

// param: PayloadModel m - model of payload, that PayloadService will use to store payloads in Mongo DB
// responsibility: get the Message, parse it and extracts Payload. Saves Payoad in MongoDB
// and then send the message to Kafka
// return: PayloadService api
PayloadService = function (m) {

    // param: BusMessage msg - message received over Bus.
    // param: PayloadModel m - the Model, where 'payload' will be saved
    // function: handle the incoming 'message-new' Message and send result to Bud, 'payload-new' topic
    // return: void
    var handleMessage = function (msg) {

        // param: BusMessage msg
        // function: create a separate context to handle incoming message
        // return: Object context
        var init = function (msg, m) {
            return {
                m: m,
                msg: msg,
                occuredAt: MyDates.now()
            };
        };

        var extract = function(ctx) {
            ctx.originalMsg = JSON.parse(ctx.msg.value);
            ctx.originalPayload = JSON.parse(ctx.originalMsg.payload);
            console.log('EXTRACTED');
            console.log(ctx.originalPayload);
        };

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
            Bus.send('payload-new', {message: result});
        };

        var ctx = init(msg, m);
        extract(ctx);
        map(ctx);
        store(ctx, notify);
    };
    Bus.subscribe('message-new', handleMessage);
}(PayloadModel);

module.exports = PayloadService;