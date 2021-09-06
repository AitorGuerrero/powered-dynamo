/* tslint:disable */
import {DynamoDB} from 'aws-sdk';

import DocumentClient = DynamoDB.DocumentClient;

export class InternalServerError extends Error {
    constructor() {
        super();
        this.name = "InternalServerError"
    }
}

export class TransactionCanceledException extends Error {
    constructor(errors: (ConditionalCheckFailed | TransactionConflict)[]) {
        super(`Transaction cancelled, please refer cancellation reasons for specific reasons [${errors.map((e) => e.name).join(",")}]`);
        this.name = "TransactionCanceledException"
    }
}

export class ConditionalCheckFailed extends Error {
    constructor() {
        super();
        this.name = "ConditionalCheckFailed"
    }
}

export class TransactionConflict extends Error {
    constructor() {
        super();
        this.name = "TransactionConflict"
    }
}

export default class FakeDocumentClient {

    public scanQueueBatches: DynamoDB.DocumentClient.ScanOutput[] = [];
    private pendingFails: {[functionName: string]: Error[]} = {
        put: [],
        transactWrite: [],
    };

    public put(i: DocumentClient.PutItemInput) {
        return {
            promise: async () => {
                const error = this.pendingFails.put.pop() || null;
                if (error) throw error;
            },
        };
    }
    public transactWrite(i: DocumentClient.TransactWriteItemsInput) {
        return {
            promise: async () => {
                const error = this.pendingFails.transactWrite.pop() || null;
                if (error) throw error;
            },
        };
    }

    public scan(i: DocumentClient.ScanInput) {
        return {
            promise: async () => this.scanQueueBatches.shift(),
        };
    }

    public failOn(functionName: string, error: Error) {
        this.pendingFails[functionName].push(error);
    }
}
