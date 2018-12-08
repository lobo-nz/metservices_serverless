package com.amazonaws.lambda.parsesavecsv;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.opencsv.CSVReader;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

	private final String TABLE_DIRECTION = System.getenv("TABLE_DIRECTION");
	private final String TABLE_HEIGHT = System.getenv("TABLE_HEIGHT");
	private final String TABLE_TEMPERATURE = System.getenv("TABLE_TEMPERATURE");
	private final String TABLE_SPEED = System.getenv("TABLE_SPEED");

	public LambdaFunctionHandler() {
	}

	// Test purpose only.
	LambdaFunctionHandler(AmazonS3 s3) {
		this.s3 = s3;
	}

	@Override
	public String handleRequest(S3Event event, Context context) {
		context.getLogger().log("Received event: " + event);

		// Get the object from the event and show its content type
		String bucket = event.getRecords().get(0).getS3().getBucket().getName();
		String key = event.getRecords().get(0).getS3().getObject().getKey();

		try {
			S3Object response = s3.getObject(new GetObjectRequest(bucket, key));
			String contentType = response.getObjectMetadata().getContentType();
			// context.getLogger().log("CONTENT TYPE: " + contentType);

			parse(response);

			return contentType;

		} catch (Exception e) {
			e.printStackTrace();
			context.getLogger().log(String.format("Error getting object %s from bucket %s. Make sure they exist and"
					+ " your bucket is in the same region as this function.", key, bucket));
			throw e;
		}
	}

	private void parse(S3Object response) {

		AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().build();
		DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
		Table tableDirection = dynamoDB.getTable(TABLE_DIRECTION);
		Table tableHeight = dynamoDB.getTable(TABLE_HEIGHT);
		Table tableTemperature = dynamoDB.getTable(TABLE_TEMPERATURE);
		Table tableSpeed = dynamoDB.getTable(TABLE_SPEED);

		try {
			Reader reader = new InputStreamReader(response.getObjectContent());
			CSVReader csvReader = new CSVReader(reader, ',', '\'', 1);

			String keyFormatPattern = "yyyyMMddHHmmss";
			String origFormatPattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";
			String dateFormatPattern = "dd-MM-yyyy HH:mm:ss";

			String[] line;
			while ((line = csvReader.readNext()) != null) {

				String stringKey = parseDate(line[0], origFormatPattern, keyFormatPattern);
				String formatDate = parseDate(line[0], origFormatPattern, dateFormatPattern);

				UpdateItemSpec updateItemSpecHeight = new UpdateItemSpec().withPrimaryKey("id", stringKey)
						.withUpdateExpression("set #na = :val1, #na2 = :val2")
						.withNameMap(new NameMap().with("#na", "sea_surface_wave_significant_height").with("#na2",
								"datetime"))
						.withValueMap(new ValueMap().with(":val1", returnValidated(line[1])).with(":val2", formatDate))
						.withReturnValues(ReturnValue.ALL_NEW);
				tableHeight.updateItem(updateItemSpecHeight);

				UpdateItemSpec updateItemSpecTemp = new UpdateItemSpec().withPrimaryKey("id", stringKey)
						.withUpdateExpression("set #na = :val1, #na2 = :val2")
						.withNameMap(new NameMap().with("#na", "air_temperature_at_2m_above_ground_level").with("#na2",
								"datetime"))
						.withValueMap(new ValueMap().with(":val1", returnValidated(line[2])).with(":val2", formatDate))
						.withReturnValues(ReturnValue.ALL_NEW);
				tableTemperature.updateItem(updateItemSpecTemp);

				UpdateItemSpec updateItemSpecDirec = new UpdateItemSpec().withPrimaryKey("id", stringKey)
						.withUpdateExpression("set #na = :val1, #na2 = :val2")
						.withNameMap(new NameMap().with("#na", "wind_from_direction_at_10m_above_ground_level")
								.with("#na2", "datetime"))
						.withValueMap(new ValueMap().with(":val1", returnValidated(line[3])).with(":val2", formatDate))
						.withReturnValues(ReturnValue.ALL_NEW);
				tableDirection.updateItem(updateItemSpecDirec);

				UpdateItemSpec updateItemSpecSpeed = new UpdateItemSpec().withPrimaryKey("id", stringKey)
						.withUpdateExpression("set #na = :val1, #na2 = :val2")
						.withNameMap(new NameMap().with("#na", "wind_speed_at_10m_above_ground_level").with("#na2",
								"datetime"))
						.withValueMap(new ValueMap().with(":val1", returnValidated(line[4])).with(":val2", formatDate))
						.withReturnValues(ReturnValue.ALL_NEW);
				tableSpeed.updateItem(updateItemSpecSpeed);

			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static String parseDate(String unformattedDate, String origFormatPattern, String destFormatPattern) {

		DateFormat dfOrigin = new SimpleDateFormat(origFormatPattern); // "yyyy-MM-dd'T'HH:mm:ss'Z'"
		// dfOrigin.setTimeZone(TimeZone.getTimeZone("GMT+13:00"));
		DateFormat dfDestination = new SimpleDateFormat(destFormatPattern);

		Date date = null;
		try {
			date = dfOrigin.parse(unformattedDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return dfDestination.format(date);
	}

	static private Double returnValidated(String value) {
		if (value != null && value.length() > 0 && !value.equals("null")) {
			return new Double(value);
		}
		return new Double(0);
	}
}