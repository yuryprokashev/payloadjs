/**
 *Created by py on 08/12/2016
 */

'use strict';
const mongoose = require( 'mongoose' );

const payloadSchema = new mongoose.Schema( {

    _id: {
        type: String,
        required: true
    }, // -> guid(), added on server when saved to Payloads collection

    type: {
        type: Number,
        required: true,
        get: v => Math.round(v),
        set: v => Math.round(v)
    }, // -> type id of the Payload. 1 for Expense. Derived by PayloadService

    amount: {
        type: Number,
        required: false
    }, // -> received from Client in payload.

    dayCode: {
        type: String,
        required: false
    }, // -> received from Client in payload.

    monthCode: {
        type: String,
        required: true
    }, // received from Client in payload.

    description: {
        type: String,
        required: false
    }, // -> received from Client in payload.

    labels: {

        isPlan: {type: Boolean, required: true}, // -> Boolean indicator of Planned (budgeted payload)

        isDeleted: {type: Boolean, required: true, default: false} // -> Boolean indicator of Deleted payload (not to be send to Client)
    },

    occurredAt: {
        type: Number,
        required: true,
        get: v => Math.round(v),
        set: v => Math.round(v)
    }, // -> milliseconds from 1-Jan-1970, added on PayloadService, when the Message is just received.

    storedAt: {
        type: Number,
        required: true,
        get: v => Math.round(v),
        set: v => Math.round(v)
    }, // -> milliseconds from 1-Jan-1970, added on PayloadService, when saved to Payload collection.

    sourceId: {
        type: Number,
        required: true,
        get: v => Math.round(v),
        set: v => Math.round(v)
    }, // -> indicator of the Message source system, received from client in Message header

    campaignId: {
        type: String,
        required: false
    }, // -> indicator of the marketing campaign, which generated the Message

    userId: {
        type: String,
        required: true
    }, // -> indicator of a User, generated the Message, received from client in Message header

    messageId: {
        type: String,
        required: false
    }, // indicator of Message that brought the Payload

    userToken: {
        type: String,
        required: false
    }, // -> userToken - unique token sent to identify where to send reply on Message

    commandId: {
        type: String,
        required: false
    } // -> command Id - unique id, used to identify command, sent from client
});

payloadSchema.set( 'toObject', { virtuals: true });
payloadSchema.set( 'toJSON', { virtuals: true });

module.exports = payloadSchema;