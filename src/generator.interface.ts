import {DynamoDB} from "aws-sdk";

import DocumentClient = DynamoDB.DocumentClient;

export default interface IGenerator
	extends Iterator<Promise<DocumentClient.AttributeMap>>, Iterable<Promise<DocumentClient.AttributeMap>> {
	count(): Promise<number>;
	toArray(): Promise<DocumentClient.AttributeMap[]>;
	slice(amount: number): Promise<DocumentClient.AttributeMap[]>;
}
