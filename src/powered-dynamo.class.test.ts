import {DynamoDB} from "aws-sdk";
import {expect} from "chai";
import {beforeEach, describe, it} from "mocha";
import MaxRetriesReached from "./error.max-retries-reached.class";
import FakeDocumentClient, {
	InternalServerError,
	TransactionCanceledException,
	TransactionConflict,
} from "./fake.document-client.class";
import PoweredDynamo from ".";

describe("PoweredDynamoClass", () => {

	const tableName = "tableName";

	let poweredDynamo: PoweredDynamo;
	let fakeDocumentClient: FakeDocumentClient;

	beforeEach(() => {
		fakeDocumentClient = new FakeDocumentClient();
		poweredDynamo = new PoweredDynamo(fakeDocumentClient as any);
		poweredDynamo.retryWaitTimes = [1, 1, 1];
	});

	describe("When putting data", () => {
		describe("Having once a system error", () => {
			beforeEach(() => fakeDocumentClient.failOn("put", new InternalServerError()));
			it("should retry the call", async () => poweredDynamo.put({TableName: tableName, Item: null as any}));
		});
		describe("Having 4 times a system error", () => {
			beforeEach(() => {
				for (let i = 0; i < 4; i++) {
					fakeDocumentClient.failOn("put", new InternalServerError());
				}
			});
			it("should throw max retries error", async () => poweredDynamo.put({TableName: tableName, Item: null as any}).then(
				() => expect.fail(),
				(err) => expect(err).instanceOf(MaxRetriesReached),
			));
		});
	});

	describe("When saving transactional data", () => {
		describe("Having once a transaction collision error", () => {
			beforeEach(() => fakeDocumentClient.failOn(
				"transactWrite",
				new TransactionCanceledException([new TransactionConflict()])),
			);
			it("should retry the call", async () => poweredDynamo.transactWrite({TransactItems: []}));
		});
		describe("Having 3 times a transaction collision error", () => {
			beforeEach(() => {
				for (let i = 0; i < 4; i++) {
					fakeDocumentClient.failOn(
						"transactWrite",
						new TransactionCanceledException([new TransactionConflict()]),
					);
				}
			});
			it("should throw max retries error", async () => poweredDynamo.transactWrite({TransactItems: []}).then(
				() => expect.fail(),
				(err) => expect(err).instanceOf(MaxRetriesReached),
			));
		});
	});
	describe('and asked for scan count with "select" attribute different from "COUNT"', () => {
		let error: Error | undefined;
		beforeEach(() => poweredDynamo.scanCount({TableName: tableName}).catch((err) => error = err));
		it('should fail', async () => expect(error).to.be.instanceof(Error));
	});
	describe('and asked for query count with "select" attribute different from "COUNT"', () => {
		let error: Error | undefined;
		beforeEach(() => poweredDynamo.queryCount({TableName: tableName}).catch((err) => error = err));
		it('should fail', async () => expect(error).to.be.instanceof(Error));
	});
	describe('document client having 2 count scan batches with a total of 11 counted items', () => {
		beforeEach(() => fakeDocumentClient.scanQueueBatches = [
			{Count: 3, LastEvaluatedKey: {}},
			{Count: 8},
		]);
		describe('and asked for scan count', () => {
			let response: number;
			beforeEach(async () => response = await poweredDynamo.scanCount({TableName: tableName, Select: 'COUNT'}));
			it('should return 11', () => expect(response).to.be.equal(11));
		})
	});
	describe('document client having 2 query response batches', () => {
		const item0Id = 'item0Id';
		const item1Id = 'item1Id';
		beforeEach(() => fakeDocumentClient.queryQueueBatches = [
			{Items: [{id: item0Id}], LastEvaluatedKey: {}},
			{Items: [{id: item1Id}]},
		]);
		describe('and asking for all the items', () => {
			const items: DynamoDB.DocumentClient.AttributeMap[] = [];
			beforeEach(async () => {
				for await (const item of poweredDynamo.query({TableName: tableName})) {
					items.push(item);
				}
			});
			it('should return all the items', () => {
				expect(items).to.have.length(2);
				expect(items[0].id).to.be.equal(item0Id);
				expect(items[1].id).to.be.equal(item1Id);
			});
		});
	});
	describe('document client having 2 scan response batches', () => {
		const item0Id = 'item0Id';
		const item1Id = 'item1Id';
		beforeEach(() => fakeDocumentClient.scanQueueBatches = [
			{Items: [{id: item0Id}], LastEvaluatedKey: {}},
			{Items: [{id: item1Id}]},
		]);
		describe('and asking for all the items', () => {
			const items: DynamoDB.DocumentClient.AttributeMap[] = [];
			beforeEach(async () => {
				for await (const item of poweredDynamo.scan({TableName: tableName})) {
					items.push(item);
				}
			});
			it('should return all the items', () => {
				expect(items).to.have.length(2);
				expect(items[0].id).to.be.equal(item0Id);
				expect(items[1].id).to.be.equal(item1Id);
			});
		});
	});
	describe('document client having 2 count query batches with a total of 11 counted items', () => {
		beforeEach(() => fakeDocumentClient.queryQueueBatches = [
			{Count: 3, LastEvaluatedKey: {}},
			{Count: 8},
		]);
		describe('and asked for scan count', () => {
			let response: number;
			beforeEach(async () => response = await poweredDynamo.queryCount({TableName: tableName, Select: 'COUNT'}));
			it('should return 11', () => expect(response).to.be.equal(11));
		})
	});
});
