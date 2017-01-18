/**
 *Created by py on 13/01/2017
 */
'use strict';

module.exports = c => {
    return `mongodb://${c.login}:${c.pwd}@${c.host}/${c.name}?authSource=${c.authSource}&authMechanism${c.authMechanism}`;
};