/**
 *Created by py on 08/12/2016
 */

'use strict';
module.exports = db => {
    let Payload = db.model("Payload", require('./payloadSchema.es6'), 'payloads');
    const guid = require('./guid.es6');
    const MonthData = require('./MonthData.es6');

    const initPayload = () =>{
        return {
            _id: guid(),
            type: undefined,
            amount: undefined,
            dayCode: undefined,
            monthCode: undefined,
            description: undefined,
            labels: {
                isDeleted: undefined,
                isPlan: undefined
            },
            occurredAt: undefined,
            storedAt: undefined,
            sourceId: undefined,
            campaignId: undefined,
            userId: undefined,
            messageId: undefined,
            userToken: undefined,
            commandId: undefined
        }
    };

    // @function: operates inside the Promise that is returned by all methods of PayloadService
    // @param: query - object that will be passed to mongoose to query Mongo. If undefined, all records will be returned.
    // @param:  sortOrder - object that will be passed to mongoose to sort the results of query. If undefined, objects will be sorted by 'occuredAt' from Z to A.
    // @param: resolve - Promise function
    // @return: resolves or rejects the Promise, where executed.
    const find = (query, data, resolve, reject) => {
        if(query === undefined) {
            query = {};
        }
        if(resolve === undefined || reject === undefined) {
            throw new Error('find function works inside Promise. Pass resolve and Reject functions as arguments')
        }
        if(typeof resolve !== 'function' || typeof reject !== 'function') {
            throw new Error('find function works inside Promise. Resolve and Reject passed are not functions');
        }

        let sortOrder;
        sortOrder = query.sortOrder;
        delete query['sortOrder'];

        console.log(`inside find method: \n ${JSON.stringify(query)} \n ${JSON.stringify(sortOrder)}`);

        Payload.find(query).sort(sortOrder).exec(
            (err, result) => {
                if(err){reject({error: `failed to find payloads with this query ${JSON.stringify(query)}`})};
                resolve(result);
            }
        )
    };
    

    const createOrUpdate = (query, data, resolve, reject) => {
        if(data === undefined) {
            reject({error: 'data is undefined, nothing to create'});
        }
        if(resolve === undefined || reject === undefined) {
            throw new Error('find function works inside Promise. Pass resolve and Reject functions as arguments')
        }
        if(typeof resolve !== 'function' || typeof reject !== 'function') {
            throw new Error('find function works inside Promise. Resolve and Reject passed are not functions');
        }
        Payload.findOneAndUpdate(
            query,
            data,
            {new: true, upsert: true},
            (err, result) => {
                if(err){reject({error:'failed to create or update payload'});}
                resolve(result);
            }
        )
    };
    
    const aggregate = (aggQuery, data, resolve, reject) => {
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

    const copy = (query, data, resolve, reject) => {

        payloadService.handle('find', query, undefined).then(
            (result) => {
                let copies = result.map((item)=>{
                    console.log(item);

                    data.dayCode = `${data.monthCode}${item.dayCode.substring(6,8)}`;

                    return payloadService.handle('createOrUpdate',{}, copyPayload(item, data)).then(
                        (result) => {
                            console.log(result);
                        },
                        (error) => {
                            reject({error: error});
                        }
                    );
                });
                resolve(Promise.all(copies));
            },
            (error) => {
                reject({error: error});
            }
        );

    };

    const copyPayload = (source, data) => {
        let copy, sourceProps, newProps;
        copy = initPayload();
        sourceProps = Object.keys(source);
        console.log(sourceProps);
        for(let sp in sourceProps) {
            if(copy.hasOwnProperty(sp) === true && sp !== '_id') {
                copy[sp] = source[sp];
            }
            else {
                console.log('source property mismatch in copyPayload');
            }
        }

        newProps = Object.keys(data);
        console.log(newProps);
        for(let p in newProps) {
            if(copy.hasOwnProperty(p) === true){
                copy[p] = data[p];
            }
            else {
                console.log('new data property mismatch in copyPayload');
            }
        }
        return copy;
    };

    let methods = new Map();
    methods.set('find', find);
    methods.set('createOrUpdate', createOrUpdate);
    methods.set('aggregate', aggregate);
    methods.set('copy', copy);

    
    const payloadService = {};

    payloadService.handle = (method, query, data) => {
        return new Promise(
            (res, rej) => {
                methods.get(method)(query, data, res, rej);
            }
        )
    };

    return payloadService;
};