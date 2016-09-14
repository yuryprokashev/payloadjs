/**
 * Created by py on 23/07/16.
 */

var MyDates;

var TimeWindow = require('./TimeWindow');

MyDates = (function() {

    // param: String string - string representation of timeWindow
    // function: extract exact date (1-31) from timeWindow
    // return: Number date
    var getDate = function(string) {
        if(string.length === 8){
            return Number(string.substring(6,8));
        }
        else {
            return null;
        }
    };

    // param: String string - string representation of timeWindow
    // function: extract Month from timeWindow
    // return: String month
    var getMonth = function(string) {
        if(string.length >=  6) {
            return Number(string.substring(4,6));
        }
        else {
            return null;
        }
    };

    // param: String string - string representation of timeWindow
    // function: extract Year from timeWindow
    // return: String year
    var getYear = function(string) {
        if(string.length >= 4) {
            return Number(string.substring(0, 4));
        }
        else {
            return null;
        }
    };

    // param: String sting - formatted TimeWindow string in "YYYYMM" or "YYYYMMDD" expected.
    // function: calculate number of days in given month
    // return: integer
    var daysInMonth = function(string) {
        var number = 30;
        var month = getMonth(string);
        var year = getYear(string);

        if(month <= 7) {
            if(month % 2 === 1) {
                number = 31;
            }
            else if(month % 2 === 0) {
                if(year % 4 === 0 && month === 2) {
                    number = 29;
                }
                else if (year % 4 !== 0 && month === 2){
                    number = 28;
                }
                else if(year % 4 !== 0 && month !== 2) {
                    number = 30
                }
            }
        }
        else if(month > 7) {
            if(month % 2 === 0) {
                number = 31
            }
        }

        return number;
    };

    // param: String t - string representation of TimeWindow object. "YYYYMM" format expected.
    // function: find out, how many weeks in given month-year.
    // return: int numberOfWeeks
    var weeksInMonth = function(t) {
        var weeks = 1;
        var firstDay = firstDayOfMonth(t);
        var daysInFirstWeek = 7 - firstDay;
        var fullWeeksInMonth = Math.floor((daysInMonth(t) - daysInFirstWeek)/7);
        var lastWeek = (daysInMonth(t) - daysInFirstWeek)%7 === 0 ? 0 : 1;
        return weeks + fullWeeksInMonth +lastWeek;

    };

    // param: String t - string representation of TimeWindow object
    // function: find out the first dat of the month
    // return: int firstDay
    var firstDayOfMonth = function(t){
        var tw = stringToTimeWindow(t);
        var firstDayAsDate = new Date(tw.year, tw.month - 1, 1);
        return firstDayAsDate.getDay();
    };

    // param: Date object
    // param: Object def - def vector, showing, what format of the TimeWindowString we need as result.
    // def has format {y: 1, m: 1, d: undefined} (this means, we want to convert date to YYYYMM string only.
    // if no 'def' is provided to function, it will consider YYYYMMDD as resulting string format.
    // function: parse date object and return it's TimeWindow string representation
    // return: String t
    var dateToTimeWindowString = function(date, def){
        if(!def) {
            def = {y:1, m:1, d: 1};
        }
        var t;
        var tw = new TimeWindow({
            year: date.getFullYear() * def.y,
            month: (date.getMonth() + 1) * def.m,
            day: date.getDate() * def.d
        });

        return timeWindowToFormattedString(tw, '');
    };

    // param: String string
    // function: convert string of allowed format to TimeWindow object
    // return: TimeWindow object
    function stringToTimeWindow (string) {
        var t;
        var allowedLengths = [0, 4, 6, 8];
        var isAllowedLength = function(element, index, array) {
            return element === string.length;
        };
        if(!allowedLengths.some(isAllowedLength)) {
            throw new Error('TimeWindow string must be 4,6 or 8 chars length');
        }
        else {
            if(string.slice(0,4) === "0000") {
                return new TimeWindow();
            }
            var year = string.slice(0,4);
            var month = string.slice(4,6);
            var day = string.slice(6,8);
            t = new TimeWindow().set({year: Number(year), month: Number(month), day: Number(day)});
            return t;
        }
    }

    // param: TimeWindow t
    // param: String s - symbol to separate 'year', 'month'' and 'day' in formatted timeWindow
    // function: format TimeWindow object to String, where 'year', 'monht' and 'day' are separated by s
    // if you omit 's', you'll get String representation of TimeWindow, used in api calls.
    // return: String
    var timeWindowToFormattedString = function(t, s) {
        if(t.year) {
            var year = t.year.toString();
        }

        if(t.month) {
            var month = t.month.toString();
            if (month.length < 2) {
                month = "0" + month;
            }
        }
        if(t.day) {
            var day = t.day.toString();
            if(day.length < 2) {
                day = "0" + day;
            }
        }
        if(!t.year) {
            return "0000";
        }
        else if(!t.month) {
            return year;
        }
        else if(!t.day){
            // return month + s + year;
            return year + s + month;
        }
        else {
            // return day + s + month + s + year;
            return year + s + month + s + day;
        }
    };

    // param: Array [TimeWindow] arr
    // param: String s - formatter
    // function: convert array of TimeWindows as heading Strings
    // return: Array [String] - array of strings, formatted with formatter separator.
    function headingsArray (arr, s) {
        var result = [];
        for(var i in arr) {
            result.push(timeWindowToFormattedString(arr[i], s));
        }
        return result;
    }

    // param: String t - timeWindow string representation
    // param: Array [int] range - the range of items to be delivered. Index 0 - is current.
    // Must contain only 2 items. [-1,1] means I want to explode t to three timeWindows:
    // -1 tw from current, current tw and +1 tw from current
    // function: convert TimeWindow to array of embedded TimeWindows.
    // return: Array [TimeWindow]
    var explode = function(t, range){
        var result = [];

        var today = new Date();

        var todayYear = today.getFullYear();

        var todayMonth = today.getMonth();

        var todayDate = today.getDate();

        var timeWindow = stringToTimeWindow(t);

        if(!timeWindow.year) {
            for (var y = range[1]; y >= range[0]; y --){
                var tw = new TimeWindow().set({year: todayYear + y});
                result.push(tw);
            }
        }
        else if(timeWindow.year && !timeWindow.month) {
            for(var m = range[1]; m >= range[0]; m--) {
                result.push(new TimeWindow().set({year: timeWindow.year, month: todayMonth + m}));
            }
        }
        else if(timeWindow.month && !timeWindow.day) {
            for(var d = range[1]; d >= range[0]; d--) {
                result.push(new TimeWindow().set({year: timeWindow.year, month: timeWindow.month, day: todayDate + d}));
            }
        }
        else if(timeWindow.day) {
            throw new Error('TimeWindows with "day" property can not be exploded');
        }

        return result;
    };

    // param: String t
    // function: find the dimension of lower level, in which to explode
    // return: int level
    var explosionIndex = function(t) {
        var index = {y: 0, m: 0, d: 0};
        if(t === "0000") {
            index = {y: 1, m: 0, d: 0};
        }
        else if(t !== "0000" && t.length === 4) {
            index = {y: 0, m: 1, d: 0};
        }
        else if(t !== "0000" && t.length === 6) {
            index = {y: 0, m: 0, d: 1};
        }
        else {
            throw new Error('Unexpected length of TimeWindow (4 or 6 expected)');
        }
        return index;
    };

    var definitiveIndex = function(t) {
        var index = {y: undefined, m: undefined, d: undefined};
        if(t === "0000") {
            index.y = 1;
        }
        else if(t !== "0000" && t.length === 4) {
            index.y = 1;
            index.m = 1;
        }
        else if(t !== "0000" && t.length === 6) {
            index.y = 1;
            index.m = 1;
            index.d = 1;
        }
        else {
            throw new Error('Unexpected length of TimeWindow (4 or 6 expected)');
        }
        return index;
    };

    // param: String t - timeWindow string representation
    // param: Array [int] range - the range of items to be delivered. Index 0 - is current.
    // param: String start - timeWindow, which is zero point in array.
    // Must contain only 2 items. [-1,1] means I want to explode t to three timeWindows:
    // -1 tw from start, current tw and +1 tw from start
    // function: convert TimeWindow to array of embedded TimeWindows.
    // return: Array [TimeWindow]
    var explode2 = function(t, range, start) {
        var result = [];
        var zero = {}; // ->  TimeWindow from which startDate we will iterate back anf forth
        var k = explosionIndex(t); // -> explosion index vector
        var def = definitiveIndex(t); // -> index, showing what is defined and what it not in result of explosion
        if(!start) {
            var today = new Date();
            zero = new TimeWindow().set({
                year: today.getFullYear() * def.y,
                month: (today.getMonth() + 1) * def.m,
                day: today.getDate() * def.d
            });
        }
        else {
            zero = stringToTimeWindow(start);
        }
        for(var i = range[1]; i >= range[0]; i--) {
            var newDate = new Date(zero.startDate.getFullYear() + k.y * i, zero.startDate.getMonth() + k.m * i, zero.startDate.getDate() + k.d * i);
            result.push(new TimeWindow().set({
                year: newDate.getFullYear() * def.y,
                month: (newDate.getMonth() + 1) * def.m,
                day: newDate.getDate() * def.d
            }));
        }
        return result;
    };

    // param: String string - timeWindow string representation
    // function: get the length of parent timeWindow string
    // return: int length - length of parent timeWindow string
    var parentLength = function(string) {
        if(string.length === 4) {
            return 4;
        }
        if(string.length === 6) {
            return 4;
        }
        if(string.length === 8) {
            return 6;
        }
    };

    // param: String string - timeWindow string representation
    // function: parse timeWindow and return it's parent as string
    // return: String - parent timeWindow string representation
    var getParent = function(string) {
        if(string === "0000") {
            return null
        }
        else if(string.length === 4 && string !== "0000") {
            return "0000";
        }
        else {
            return string.slice(0, parentLength(string));
        }
    };

    // param: String t - timeWindow string representation
    // param: Array [int] range - the range of items to be delivered. Index 0 - is current.
    // Must contain only 2 items. [-1,1] means I want three neighbours of t:
    // -1 tw from current, current tw and +1 tw from current
    // function: constructs the array of several Months: range[0] back, current, range[1] forward.
    // return: Array [TimeWindow]
    var getNeighbours = function (t, range) {
        var result = [];
        var parent = getParent(t);
        result = explode2(parent, range, t);
        return result;
    };

    // param: int dayNum
    // function: make char string out of integer
    // return: String day
    var dayToString = function(dayNum){
        var day;
        if(dayNum > 0 && dayNum <= 9){
            day = "0" + dayNum.toString();
        }
        else if(dayNum > 9) {
            day = dayNum.toString();
        }
        return day;
    };

    // param: Date date - date object
    // function: calculate the week number in month, that is encoded in given Date object (from 0 to 3-5)
    // return: int result
    var numberOfWeek = function(date) {
        var result = null;
        var tws = dateToTimeWindowString(date, {y: 1, m: 1, d: undefined}); // -> this is currentMonth, encoded with string
        // var days = daysInMonth(tws); // -> int number of days in month
        var fday = firstDayOfMonth(tws); // -> int first day of month
        var today = getDate(dateToTimeWindowString(date, {y: 1, m: 1, d: 1})); // -> int today date
        return Math.floor((today + fday - 1)/7);

    };

    // param: void
    // function: shorten and unify the DateTime format used for precise timing
    // return: int datetime value in milliseconds
    var nowInMilliseconds = function() {
        return new Date().valueOf();
        // return Date.parse(new Date());
    };

    var monthAsLabel = function(t, isLong) {

        var tw = stringToTimeWindow(t);

        if(tw.year && tw.month && !tw.day) {
            var monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
            var longMonthNames = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
            var year = tw.year.toString().slice(2);
            var month;
            if(isLong) {
                month = longMonthNames[tw.month - 1];
            }
            else {
                month = monthNames[tw.month - 1];
            }
            return month + '-' + year;
        }
        else if(tw.year && tw.month && tw.day) {
            throw new Error('this function works only for TimeWindows on month level');
        }
    };

    return {
        daysInMonth: daysInMonth,
        weeksInMonth: weeksInMonth,
        firstDay: firstDayOfMonth,
        dateToString: dateToTimeWindowString,
        getYearFromString: getYear,
        getMonthFromString: getMonth,
        getDateFromString: getDate,
        twToString: timeWindowToFormattedString,
        stringToTw: stringToTimeWindow,
        explode: explode2,
        headingsArray: headingsArray,
        parent: getParent,
        neighbours: getNeighbours,
        dayToString: dayToString,
        numberOfWeek: numberOfWeek,
        now: nowInMilliseconds,
        monthAsLabel: monthAsLabel
    }
})();

module.exports = MyDates;