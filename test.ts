import {expect} from "chai";
import {beforeEach, describe, it} from "mocha";
import MaxRetriesReached from "./error.max-retries-reached.class";
import FakeDocumentClient, {
	InternalServerError,
	TransactionCanceledException,
	TransactionConflict,
} from "./fake.document-client.class";
import PoweredDynamo from "./index";

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
			it("should retry the call", async () => poweredDynamo.put({TableName: tableName, Item: null}));
		});
		describe("Having 4 times a system error", () => {
			beforeEach(() => {
				for (let i = 0; i < 4; i++) {
					fakeDocumentClient.failOn("put", new InternalServerError());
				}
			});
			it("should throw max retries error", async () => poweredDynamo.put({TableName: tableName, Item: null}).then(
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

	describe("When requesting scanned data", () => {
		describe("and scan returns 2 batches and 5 elements total", () => {
			const firstElementId = "firstElementId";
			const thirdElementId = "thirdElementId";
			beforeEach(() => fakeDocumentClient.scanQueueBatches = [
				{Items: [{id: firstElementId}, {id: "itemA"}], LastEvaluatedKey: {}},
				{Items: [{id: thirdElementId}, {id: "itemB"}, {id: "itemC"}], LastEvaluatedKey: {}},
			]);
			describe("and asking for the first result", () => {
				it("should return first element", async () => {
					const element = await (await poweredDynamo.scan({TableName: tableName})).next().value;
					expect(element.id).to.be.equal(firstElementId);
				});
			});
			describe("and asking for the third result", () => {
				it("should return third element", async () => {
					const result = await poweredDynamo.scan({TableName: tableName});
					await result.next().value;
					await result.next().value;
					const element = await result.next().value;
					expect(element.id).to.be.equal(thirdElementId);
				});
			});
			describe("and asking for 3 items slice", () => {
				it("should return 3 first items", async () => {
					const result = await (await poweredDynamo.scan({TableName: tableName})).slice(3);
					expect(result.length).to.be.equal(3);
					expect(result[0].id).to.be.equal(firstElementId);
					expect(result[2].id).to.be.equal(thirdElementId);
				});
			});
		});
	});
});
