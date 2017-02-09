/**
 *Created by py on 08/12/2016
 */

'use strict';
module.exports = (db, EventEmitter) => {
    let Payload = db.model("Payload", require('./payloadSchema.es6'), 'payloads');
    const guid = require('./helpers/guid.es6');
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

        return Payload.find(query).sort(sortOrder);
    };

    const createOrUpdate = (query, data) => {
        console.log(`query \n ${JSON.stringify(query)}\n`);
        console.log(`data \n ${JSON.stringify(data)}\n`);
        return Payload.findOneAndUpdate(query, data, {new: true, upsert: true, setDefaultsOnInsert: true});
    };

    const aggregate = (aggQuery, data) => {
        return new Promise(
            (resolve, reject) => {
                Payload.aggregate(aggQuery).exec(
                    (err, data) => {
                        if(err) {
                            reject({error: err});
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
                    });
            });
    };

    const copy = (query, data) => {
        let copies;

        return new Promise(
            (resolve, reject) => {
                find(query, data).then(
                    (result) => {
                        copies = result.map(
                            (item) => {
                                data.dayCode = `${data.monthCode}${item._doc.dayCode.substring(6,8)}`;
                                let copy = copyPayload(item._doc, data);
                                return Payload.create(copy);
                            });
                        resolve(Promise.all(copies));
                    },
                    (error) => {
                        reject({error: error});
                    }
                );
            })
    };

    const copyPayload = (source, data) => {
        let copy, sourceProps, newProps;
        copy = initPayload();
        sourceProps = Object.keys(source);
        // console.log(sourceProps);
        for(let sp of sourceProps) {
            // console.log(sp);
            // console.log(copy.hasOwnProperty(sp));
            if(copy.hasOwnProperty(sp) === true && /(_id)|(__v)/.test(sp) === false) {
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

    const clear = (query, data) => {
        let deleted;
        return new Promise(
            (resolve, reject) => {
                find(query, data).then(
                    (result) => {
                        deleted = result.map(
                            (item) => {
                                return Payload.update({_id: item._doc._id}, {'labels.isDeleted': true}, {multi:true});
                            });
                        resolve(Promise.all(deleted));
                    },
                    (error) => {
                        reject({error: error});
                    }
                );
            })
    };

    let methods = new Map();
    methods.set('find', find);
    methods.set('createOrUpdate', createOrUpdate);
    methods.set('aggregate', aggregate);
    methods.set('copy', copy);
    methods.set('clear', clear);

    const payloadService = new EventEmitter();

    payloadService.handle = (method, query, data) => {
        return methods.get(method)(query, data);
    };

    return payloadService;
};