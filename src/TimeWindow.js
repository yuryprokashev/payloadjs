
var TimeWindow;

// param: Object args
// function: object constructor
// return: TimeWindow object

TimeWindow = function(args) {
    if(args) {
        this.set(args);
    }
    else {
        this.startDate = new Date(Date.UTC(1970,0,1));
        this.endDate = new Date(Date.UTC(2100,0,1));
        this.year = null;
        this.month = null;
        this.day = null;
    }
};

TimeWindow.prototype = {
    // param: Object args
    // function: set TimeWindow object properties based on given properties in args.
    // note: TimeWindow object is created based exactly on the given args.
    // It means that if you want March-2016, you should give {year: 2016, month: 3}
    // If you give args as formulas based on Date.getMonth(), please take into account, that Javascript Date object
    // gives you back 2 for March, not 3.
    // return: modified TimeWindow object
    set: function(args){
        const DAY = 24 * 3600 * 1000;
        if(!args) {
            throw new Error('no args provided - TimeWindow object remained unchanged');
        }
        else if(args.day) {
            if(args.month < 0) {
                if(args.day < 0) {
                    this.startDate = new Date(Date.UTC(args.year, args.month, args.day + 1));
                }
                else {
                    this.startDate = new Date(Date.UTC(args.year, args.month, args.day));
                }
            }
            else {
                if(args.day < 0) {
                    this.startDate = new Date(Date.UTC(args.year, args.month - 1, args.day + 1));
                }
                else {
                    this.startDate = new Date(Date.UTC(args.year, args.month - 1, args.day));
                }
            }
            this.endDate = new Date(Date.UTC(this.startDate.valueOf() + DAY));
            this.year = this.startDate.getFullYear();
            this.month = this.startDate.getMonth() + 1;
            this.day = this.startDate.getDate();
        }
        else if(!args.day && args.month) {

            if(args.month < 0) {
                this.startDate = new Date(Date.UTC(args.year, args.month, 1));
                this.endDate = new Date(Date.UTC(args.year, args.month + 1, 1));
            }
            else {
                this.startDate = new Date(Date.UTC(args.year, args.month - 1, 1));
                this.endDate = new Date(Date.UTC(args.year, args.month, 1));
            }
            this.year = this.startDate.getFullYear();
            this.month = this.startDate.getMonth() + 1;
            this.day = null;

        }
        else if(!args.day && !args.month && args.year) {
            this.startDate = new Date(Date.UTC(args.year,0,1));
            this.endDate = new Date(Date.UTC(args.year + 1,0,1));
            this.year = this.startDate.getFullYear();
            this.month = null;
            this.day = null;
        }

        return this;
    }
};

module.exports = TimeWindow;