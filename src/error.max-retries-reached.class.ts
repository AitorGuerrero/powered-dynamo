export default class MaxRetriesReached extends Error {
	constructor() {
		super("max writing retries reached in dynamo db");
		this.name = "PoweredDynamo.maxRetriesReached";
	}
}
