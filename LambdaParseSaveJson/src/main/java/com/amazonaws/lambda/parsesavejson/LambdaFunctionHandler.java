package com.amazonaws.lambda.parsesavejson;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import org.json.JSONObject;

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

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

	private final String TABLE_DIRECTION = System.getenv("TABLE_DIRECTION");
	private final String TABLE_HEIGHT = System.getenv("TABLE_HEIGHT");
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
			context.getLogger().log("CONTENT TYPE: " + contentType);

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
		Table tableSpeed = dynamoDB.getTable(TABLE_SPEED);

		Reader reader = new InputStreamReader(response.getObjectContent());

		String jsonData = readFile(reader);
		JSONObject jobj = new JSONObject(jsonData);

		String keyFormatPattern = "yyyyMMddHHmmss";
		String origFormatPattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";
		String dateFormatPattern = "dd-MM-yyyy HH:mm:ss";

		for (Map.Entry<String, Object> entry : jobj.toMap().entrySet()) {

			String stringKey = parseDate(entry.getKey(), origFormatPattern, keyFormatPattern);
			String formatDate = parseDate(entry.getKey(), origFormatPattern, dateFormatPattern);

			Map<String, Double> dataValues = (Map<String, Double>) entry.getValue();

			for (Map.Entry<String, Double> value : dataValues.entrySet()) {

				UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("id", stringKey)
						.withUpdateExpression("set #na = :val1, #na2 = :val2")
						.withNameMap(new NameMap().with("#na", value.getKey()).with("#na2", "datetime"))
						.withValueMap(new ValueMap().with(":val1", returnValidated(value.getValue())).with(":val2",
								formatDate))
						.withReturnValues(ReturnValue.ALL_NEW);

				switch (value.getKey()) {
				case "sea_surface_wave_from_direction_at_variance_spectral_density_maximum":
					// tableDirection.putItem(item);
					tableDirection.updateItem(updateItemSpec);
					break;
				case "surface_sea_water_speed":
					// tableSpeed.putItem(item);
					tableSpeed.updateItem(updateItemSpec);
					break;
				case "sea_surface_wave_maximum_height":
					// tableHeight.putItem(item);
					tableHeight.updateItem(updateItemSpec);
					break;
				}

			}

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

	public static String readFile(Reader reader) {
		String result = "";
		try {
			BufferedReader br = new BufferedReader(reader);
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();
			while (line != null) {
				sb.append(line);
				line = br.readLine();
			}
			result = sb.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	static private Double returnValidated(Object value) {

		if (value != null && value instanceof Double) {
			return (Double) value;
		}
		return new Double(0);
	}

}