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
    
    const find = (query, data) => {
        let sortOrder;

        sortOrder = query.sortOrder;
        delete query['sortOrder'];

        return new Promise(
            (resolve, reject) => {
                Payload.find(query).sort(sortOrder).exec(
                    (err, result) => {
                        if(err){reject({error: `failed to find payloads with this query ${JSON.stringify(query)}`})};
                        resolve(result);
                    }
                )
            }
        )
    };

    const createOrUpdate = (query, data) => {
        return new Promise(
            (resolve, reject) => {
                Payload.findOneAndUpdate(
                    query,
                    data,
                    {new: true, upsert: true},
                    (err, result) => {
                        if(err){reject({error:'failed to create or update payload'});}
                        resolve(result);
                    }
                )
            }
        )
    };

    const aggregate = (aggQuery, data) => {
        return new Promise(
            (resolve, reject) => {
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
            });
    };

    const copy = (query, data) => {
        let copies;

        find(query, data).then(
            (result) => {
                copies = result.map(
                    (item) => {
                        let copy = copyPayload(item._doc, data);
                        return createOrUpdate({}, copy);
                    }
                );
                console.log(JSON.stringify(copies));
                return Promise.all(copies);
            },
            (error) => {
                copies = error.map(
                    (item) => {
                        return item;
                    }
                );
                return Promise.reject({error: `failed to find items to copy with given query ${JSON.stringify(query)} `})
            }
        );

    };

    const copyPayload = (source, data) => {
        let copy, sourceProps, newProps;
        copy = initPayload();
        sourceProps = Object.keys(source);
        // console.log(sourceProps);
        for(let sp of sourceProps) {
            // console.log(sp);
            // console.log(copy.hasOwnProperty(sp));
            if(copy.hasOwnProperty(sp) === true && sp !== '_id') {
                copy[sp] = source[sp];
            }
            else {
                console.log('source property mismatch in copyPayload');
            }
        }

        newProps = Object.keys(data);
        // console.log(newProps);
        for(let p of newProps) {
            if(copy.hasOwnProperty(p) === true){
                copy[p] = data[p];
            }
            else {
                console.log('new data property mismatch in copyPayload');
            }
        }
        // console.log(`${JSON.stringify(copy)} \n ${JSON.stringify(data)} \n`);
        return copy;
    };

    let methods = new Map();
    methods.set('find', find);
    methods.set('createOrUpdate', createOrUpdate);
    methods.set('aggregate', aggregate);
    methods.set('copy', copy);

    
    const payloadService = {};


    payloadService.handle = (method, query, data) => {
        return methods.get(method)(query, data);
    };

    return payloadService;
};