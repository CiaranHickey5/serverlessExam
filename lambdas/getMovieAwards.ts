import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDocumentClient();

export const handler: APIGatewayProxyHandlerV2 = async (event) => {
  try {
    console.log("Event: ", JSON.stringify(event));

    // Extract path parameters
    const { awardBody, movieId } = event.pathParameters || {};
    if (!awardBody || !movieId) {
      return {
        statusCode: 400,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: "Missing required path parameters." }),
      };
    }

    // Extract query parameters
    const minAwards = event.queryStringParameters?.min
      ? parseInt(event.queryStringParameters.min)
      : null;

    // Query the MovieAwards table
    const queryCommand = new QueryCommand({
      TableName: process.env.AWARDS_TABLE_NAME,
      KeyConditionExpression: "movieId = :m AND awardBody = :a",
      ExpressionAttributeValues: {
        ":m": parseInt(movieId),
        ":a": awardBody,
      },
    });

    const commandOutput = await ddbDocClient.send(queryCommand);

    // Check if any data is present
    if (!commandOutput.Items || commandOutput.Items.length === 0) {
      return {
        statusCode: 404,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          message: "No awards found for the specified movie and award body.",
        }),
      };
    }

    // Apply minimum awards filter if provided
    const awards = commandOutput.Items;
    if (minAwards !== null) {
      const filteredAwards = awards.filter(
        (award: any) => award.numAwards > minAwards
      );

      if (filteredAwards.length === 0) {
        return {
          statusCode: 400,
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ message: "Request failed" }),
        };
      }

      return {
        statusCode: 200,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ data: filteredAwards }),
      };
    }

    // Return the full results if no filter is applied
    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ data: awards }),
    };
  } catch (error: any) {
    console.error("Error: ", error);
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        error: "An error occurred while processing the request.",
      }),
    };
  }
};

function createDocumentClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  return DynamoDBDocumentClient.from(ddbClient);
}
