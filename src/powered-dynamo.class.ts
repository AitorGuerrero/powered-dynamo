import * as assert from "assert";
import {DynamoDB} from "aws-sdk";
import DynamoGeneratorFactory from "dynamo-iterator";
import {EventEmitter} from "events";
import MaxRetriesReached from "./error.max-retries-reached.class";
import DocumentClient = DynamoDB.DocumentClient;

const maxBatchWriteElems = 25;
const maxBatchGetElems = 100;

enum EventType {
	retryableError = "retryableError",
}

export default class PoweredDynamo {

	private static splitBatchWriteRequestsInChunks(request: DocumentClient.BatchWriteItemInput): DocumentClient.BatchWriteItemRequestMap[] {
		const requests: {tableName: string, request: DocumentClient.WriteRequest}[] = [];
		const batches: DocumentClient.BatchWriteItemRequestMap[] = [];
		for (const tableName of Object.keys(request.RequestItems)) {
			for (const itemRequest of request.RequestItems[tableName]) {
				requests.push({tableName, request: itemRequest});
			}
		}
		for (let i = 0; i < requests.length; i += maxBatchWriteElems) {
			const batchRequestMap: DocumentClient.BatchWriteItemRequestMap = {};
			for (const itemRequest of requests.slice(i, i + maxBatchWriteElems)) {
				batchRequestMap[itemRequest.tableName] = [...(batchRequestMap[itemRequest.tableName] || []), itemRequest.request];
			}
			batches.push(batchRequestMap);
		}

		return batches;
	}

	private static isInternalServerError(error: unknown): boolean {
		return error instanceof Error && error.name === "InternalServerError";
	}

	private static isRetryableTransactionError(err: unknown): boolean {
		return err instanceof Error && err.name === "TransactionCanceledException" && !/ConditionalCheckFailed/.test(err.message)
			&& /TransactionConflict/.test(err.message);
	}

	private static async sumCountResponses(generator: AsyncGenerator<DynamoDB.DocumentClient.QueryOutput | DynamoDB.DocumentClient.ScanOutput>): Promise<number> {
		let count = 0;
		for await (const response of generator) {
			count += response.Count!;
		}

		return count;
	}

	public retryWaitTimes: number[] = [100, 500, 1000];

	private generatorFactory: DynamoGeneratorFactory;

	constructor(
		private documentClient: DocumentClient,
		public eventEmitter = new EventEmitter(),
	) {
		this.generatorFactory = new DynamoGeneratorFactory(documentClient);
	}

	public get(input: DocumentClient.GetItemInput) {
		return this.documentClient.get(input).promise();
	}

	public async getList(tableName: string, keys: DocumentClient.Key[]): Promise<Map<DocumentClient.Key, DocumentClient.AttributeMap | undefined>> {
		const uniqueKeys: DocumentClient.Key = filterRepeatedKeys(keys);
		const result = new Map<DocumentClient.Key, DocumentClient.AttributeMap | undefined>();
		const batchProcesses: Promise<void>[] = [];
		for (let i = 0; i < uniqueKeys.length; i += maxBatchGetElems) {
			batchProcesses.push(new Promise(async (rs, rj) => {
				try {
					const keysBatch: DocumentClient.Key[] = uniqueKeys.slice(i, i + maxBatchGetElems);
					const input: DocumentClient.BatchGetItemInput = {
						RequestItems: {[tableName]: {Keys: keysBatch}},
					};
					const response = await this.asyncBatchGet(input);
					for (const key of keys) {
						result.set(key, response.Responses![tableName].find((item) => sameKey(key, item)));
					}
					rs();
				} catch (err) {
					rj(err);
				}
			}));
		}
		await Promise.all(batchProcesses);

		return result;
	}

	public scan(input: DocumentClient.ScanInput): AsyncGenerator<DynamoDB.DocumentClient.AttributeMap> {
		return this.generatorFactory.scan(input);
	}

	public query(input: DocumentClient.QueryInput): AsyncGenerator<DynamoDB.DocumentClient.AttributeMap> {
		return this.generatorFactory.query(input);
	}

	public async scanCount(input: DocumentClient.ScanInput): Promise<number> {
		assert.equal(input.Select, 'COUNT', 'For count scan Select must be "count"');
		return PoweredDynamo.sumCountResponses(this.generatorFactory.scanResponses(input));
	}

	public async queryCount(input: DocumentClient.QueryInput): Promise<number> {
		assert.equal(input.Select, 'COUNT', 'For count query Select must be "count"');
		return PoweredDynamo.sumCountResponses(this.generatorFactory.queryResponses(input));
	}

	public put(input: DocumentClient.PutItemInput) {
		return this.retryInternalServerError(
			() => this.documentClient.put(input).promise(),
		);
	}

	public update(input: DocumentClient.UpdateItemInput) {
		return this.retryInternalServerError(
			() => this.documentClient.update(input).promise(),
		);
	}

	public delete(input: DocumentClient.DeleteItemInput) {
		return this.retryInternalServerError(
			() => this.documentClient.delete(input).promise(),
		);
	}

	public async batchWrite(request: DocumentClient.BatchWriteItemInput) {
		for (const batch of PoweredDynamo.splitBatchWriteRequestsInChunks(request)) {
			await this.retryInternalServerError(() => this.asyncBatchWrite(Object.assign(request, {RequestItems: batch})));
		}
	}

	public async transactWrite(input: DocumentClient.TransactWriteItemsInput) {
		await this.retryTransactionCancelledServerError(() =>
			this.retryInternalServerError(() => this.asyncTransactWrite(input)),
		);
	}

	private async retryTransactionCancelledServerError<O>(execution: () => Promise<O>) {
		return this.retryError(
			PoweredDynamo.isRetryableTransactionError,
			execution,
		);
	}

	private async retryInternalServerError<O>(execution: () => Promise<O>) {
		return this.retryError(
			PoweredDynamo.isInternalServerError,
			execution,
		);
	}

	private async retryError<O>(
		isRetryable: (err: unknown) => boolean,
		execution: () => Promise<O>,
		tryCount = 0,
	): Promise<O> {
		try {
			return await execution();
		} catch (error) {
			if (isRetryable(error)) {
				this.eventEmitter.emit(EventType.retryableError, error);
				if (this.retryWaitTimes[tryCount] === undefined) {
					throw new MaxRetriesReached();
				}
				await new Promise((rs) => setTimeout(rs, this.retryWaitTimes[tryCount]));
				return await this.retryError(isRetryable, execution, tryCount + 1);
			}

			throw error;
		}
	}

	private asyncBatchGet(input: DynamoDB.DocumentClient.BatchGetItemInput) {
		return this.documentClient.batchGet(input).promise();
	}

	private async asyncTransactWrite(input: DocumentClient.TransactWriteItemsInput) {
		await this.documentClient.transactWrite(input).promise();
	}

	private async asyncBatchWrite(input: DocumentClient.BatchWriteItemInput) {
		await this.documentClient.batchWrite(input).promise();
	}
}

function filterRepeatedKeys(arrArg: DocumentClient.Key[]) {
	return arrArg.reduce(
		(output, key) => output.some(
			(k2: DocumentClient.Key) => sameKey(key, k2),
		) ? output : output.concat([key]),
		[] as DocumentClient.Key[],
	);
}

function sameKey(key1: DocumentClient.Key, key2: DocumentClient.Key): boolean {
	return Object.keys(key1).every((k) => key2[k] === key1[k]);
}
