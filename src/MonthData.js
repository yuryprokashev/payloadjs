/**
 * Created by py on 24/07/16.
 */

var MonthData;

// param: int expSumFact - total expenses that are happened already
// param: int expSumPlan - total expenses that are planned
// function: object constructor. This object is constructed on Server and returned in response to client over http GET
// it is FORBIDDEN to construct it in Client code.
// return: new instance of MonthData object
MonthData = function (expSumFact, expSumPlan) {
    if(typeof expSumPlan !== 'number' || typeof expSumFact !== 'number') {
        throw new Error('expense sums should be numbers');
    }
    else {
        this.totals = {
            fact: expSumFact,
            plan: expSumPlan
        };
    }
};

module.exports = MonthData;