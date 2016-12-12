/**
 *Created by py on 08/12/2016
 */

'use strict';
module.exports = db => {
    let Payload = db.model("Payload", require('./payloadSchema.es6'), 'payloads');
    const guid = require('./guid.es6');
    const MonthData = require('./MonthData.es6');

    // @function: validates parsedMessage. Works inside the Promise. Rejects the Promise if parsedMessage is not valid.
    const validateParsedMessage = (parsedMessage, reject) => {
        if(parsedMessage === undefined) {
            reject({error: 'parsed message is undefined'});
        }
        if(parsedMessage.responseErrors.length > 0) {
            reject({error: 'incoming kafkaMessage contains errors'});
        }
    };
    
    // @function: parses parsedMessage and extracts 'payload' field out of it.
    const extractPayload = (parsedMessage) => {
        return JSON.parse(parsedMessage.payload);
    };

    // @function: constructs query object based on topic and parsedMessage
    const constructQuery = (topic, parsedMessage) => {
        switch (topic){
            case 'message-done':
                let parsedPayload = extractPayload(parsedMessage);
                return {"_id": parsedPayload._id};
            case 'payload-request':
                return {
                    type: parsedMessage.payloadType || 1,
                    dayCode: parsedMessage.dayCode !== undefined ? parsedMessage.dayCode : undefined,
                    monthCode: parsedMessage.monthCode !== undefined ? parsedMessage.monthCode : undefined,
                    userId: parsedMessage.user,
                    'labels.isDeleted': false
                };
            case 'copy-payload-request':
                return 0;
            case 'clear-payload-request':
                return 1;
            case 'get-month-data-request':
                return 2;
        }
    };

    const constructNewPayload = (topic, parsedMessage, parsedPayload) => {
        switch (topic) {
            case 'message-new':
                return {
                    _id: parsedPayload._id,
                    type: 1,
                    amount: parsedPayload.amount,
                    dayCode: parsedPayload.dayCode,
                    monthCode: parsedPayload.monthCode,
                    description: parsedPayload.description,
                    labels: parsedPayload.labels,
                    occuredAt: new Date().valueOf(),
                    sourceId: parsedMessage.responsePayload.sourceId,
                    campaignId: parsedMessage.responsePayload.campaignId || 0,
                    userId: parsedMessage.responsePayload.userId,
                    messageId: parsedMessage.responsePayload._id,
                    userToken: parsedMessage.responsePayload.userToken,
                    commandId: parsedMessage.responsePayload.commandId
                };
            case 'copy-payload-request':

        }
    };
    
    const constructAggregateQuery = (topic, parsedMessage) => {
        switch (topic) {
            case 'get-month-data-request':
                return [
                    {$match: {userId: parsedMessage.user, "labels.isDeleted": false}},
                    {$project: {_id:1, amount:1, monthCode: 1, isPlanned: {$cond:{if:{$eq:["$labels.isPlan",true]}, then:"plan", else:"fact"}}}},
                    {$match: {monthCode: parsedMessage.targetPeriod}},
                    {$project: { _id:1, amount:1,isPlanned: "$isPlanned"}},
                    {$group: {_id: "$isPlanned", total: {$sum: "$amount"}}}
                ];
        }

    };

    // @function: operates inside the Promise that is returned by all methods of PayloadService
    // @param: query - object that will be passed to mongoose to query Mongo. If undefined, all records will be returned.
    // @param:  sortOrder - object that will be passed to mongoose to sort the results of query. If undefined, objects will be sorted by 'occuredAt' from Z to A.
    // @param: resolve - Promise function
    // @return: resolves or rejects the Promise, where executed.
    const find = (query, sortOrder, resolve, reject) => {
        if(query === undefined) {
            query = {};
        }
        if(sortOrder === undefined) {
            sortOrder = { occuredAt: -1 }
        }
        if(resolve === undefined || reject === undefined) {
            throw new Error('find function works inside Promise. Pass resolve and Reject functions as arguments')
        }
        if(typeof resolve !== 'function' || typeof reject !== 'function') {
            throw new Error('find function works inside Promise. Resolve and Reject passed are not functions');
        }
        Payload.find(query).sort(sortOrder).exec(
            (err, result) => {
                if(err){reject({error: `failed to find payloads with this query ${JSON.stringify(query)}`})};
                resolve(result);
            }
        )
    };

    const createOrUpdate = (query, newItem, resolve, reject) => {
        if(newItem === undefined) {
            reject({error: 'newItem is undefined, nothing to create'});
        }
        if(resolve === undefined || reject === undefined) {
            throw new Error('find function works inside Promise. Pass resolve and Reject functions as arguments')
        }
        if(typeof resolve !== 'function' || typeof reject !== 'function') {
            throw new Error('find function works inside Promise. Resolve and Reject passed are not functions');
        }
        Payload.findOneAndUpdate(
            query,
            newItem,
            {new: true, upsert: true},
            (err, result) => {
                if(err){reject({error:'failed to create or update payload'});}
                resolve(result);
            }
        )
    };
    
    const aggregate = (aggQuery, resolve, reject) => {
        if(resolve === undefined || reject === undefined) {
            throw new Error('find function works inside Promise. Pass resolve and Reject functions as arguments')
        }
        if(typeof resolve !== 'function' || typeof reject !== 'function') {
            throw new Error('find function works inside Promise. Resolve and Reject passed are not functions');
        }
        Payload.aggregate(aggQuery).exec(
            (err, data) => {
                if(err) {
                    reject({error: `failed to aggregate payloads with query ${JSON.stringify(aggQuery)}`});
                }
                let monthData, fact, plan;
                function findPlan(item) {
                    return item._id === 'plan';
                }

                function findFact(item) {
                    return item._id === 'fact';
                }
                fact = data.find(findFact) || {_id: 'fact', total: 0};
                plan = data.find(findPlan) || {_id: 'plan', total: 0};
                monthData = new MonthData(fact.total, plan.total);

                resolve(monthData);

            }
        )
    };

    
    const payloadService = {};
    
    payloadService.createOrUpdate = (topic, parsedMessage) => {
        return new Promise(
            (res, rej) => {

                validateParsedMessage(parsedMessage, rej);

                let parsedPayload = extractPayload(parsedMessage);

                let query = constructQuery(topic, parsedMessage);

                let payload = constructNewPayload(topic, parsedMessage, parsedPayload);

                createOrUpdate(query, payload, res, rej);

            }
        );
    };

    payloadService.findPayloads = (topic, parsedMessage) => {
        return new Promise(
            (res, rej) => {

                validateParsedMessage(parsedMessage, rej);

                let query = constructQuery(topic, parsedMessage);

                let sortOrder = parsedMessage.responsePayload.sortOrder;

                find(query, sortOrder, res, rej);

            }
        );
    };

    payloadService.aggregatePayloads = (topic, parsedMessage) => {
        return new Promise(
            (res, rej) => {

                validateParsedMessage(parsedMessage, rej);

                let agg = constructAggregateQuery(topic, parsedMessage);

                aggregate(agg, res, rej);

            }
        )
    };

    return payloadService;
};