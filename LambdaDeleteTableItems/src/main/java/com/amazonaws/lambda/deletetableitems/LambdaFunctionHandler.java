package com.amazonaws.lambda.deletetableitems;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class LambdaFunctionHandler implements RequestHandler<Object, String> {

	AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().build();
	DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
	
	@Override
	public String handleRequest(Object input, Context context) {
		context.getLogger().log("Input: " + input);

		String tableDirection = System.getenv("TABLE_DIRECTION");
		String tableHeight = System.getenv("TABLE_HEIGHT");
		String tableTemperature = System.getenv("TABLE_TEMPERATURE");
		String tableSpeed = System.getenv("TABLE_SPEED");

		deleteAllItemIn(tableDirection);
		deleteAllItemIn(tableHeight);
		deleteAllItemIn(tableTemperature);
		deleteAllItemIn(tableSpeed);

		return "Done!";
	}

	private void deleteAllItemIn(String table) {
		try {
			ScanRequest scanRequest = new ScanRequest().withTableName(table);
			ScanResult scanResult = null;

			do {
				if (scanResult != null) {
					scanRequest.setExclusiveStartKey(scanResult.getLastEvaluatedKey());
				}

				scanResult = dynamoDBClient.scan(scanRequest);

				scanResult.getItems().forEach((item) -> {

					deleteItem(dynamoDB.getTable(table), item);

				});
			} while (scanResult.getLastEvaluatedKey() != null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void deleteItem(Table table, Map<String, AttributeValue> item) {
		try {

			DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey("id", item.get("id").getS())
					.withReturnValues(ReturnValue.ALL_OLD);
			DeleteItemOutcome outcome = table.deleteItem(deleteItemSpec);
			// Check the response.
			System.out.println("Printing item that was deleted...");
			System.out.println(outcome.getItem().toJSONPretty());

		} catch (Exception e) {
			System.err.println("Error deleting item in ");
			System.err.println(e.getMessage());
		}
	}

}
