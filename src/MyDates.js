/**
 * Created by py on 23/07/16.
 */

var MyDates;

MyDates = (function() {

    // param: void
    // function: shorten and unify the DateTime format used for precise timing
    // return: int datetime value in milliseconds
    var nowInMilliseconds = function() {
        return new Date().valueOf();
        // return Date.parse(new Date());
    };

    return {
        now: nowInMilliseconds,
    }
})();

module.exports = MyDates;