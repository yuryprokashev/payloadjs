/**
 * Created by py on 03/11/2016.
 */
'use strict';
function parseProcessAgrs(){
    let args = [];
    process.argv.forEach(function (arg) {
        args.push(arg);
    });
    let sliced = args.slice(2);
    let result = [];
    sliced.forEach(function(arg){
        let argSplit = arg.split('=');
        let argObj = {};
        argObj[argSplit[0]] = Number(argSplit[1]);
        result.push(argObj);
    });

    return result;
}
module.exports = parseProcessAgrs;
