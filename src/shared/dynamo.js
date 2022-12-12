const AWS = require("aws-sdk");
const get = require("lodash.get");
var dynamodb = new AWS.DynamoDB.DocumentClient();

async function getItem(tableName, key, attributesToGet = null) {
  let params;
  try {
    params = {
      TableName: tableName,
      Key: key,
    };
    if (attributesToGet) params.AttributesToGet = attributesToGet;
    return await dynamodb.get(params).promise();
  } catch (e) {
    console.error("Get Item Error: ", e, "\nGet params: ", params);
    throw "GetItemError";
  }
}

async function putItem(tableName, item) {
  let params;
  try {
    params = {
      TableName: tableName,
      Item: item,
    };
    return await dynamodb.put(params).promise();
  } catch (e) {
    console.error("Put Item Error: ", e, "\nPut params: ", params);
    throw "PutItemError";
  }
}

async function updateItem(tableName, key, item, operation = "SET") {
  let params;
  try {
    const [expression, expressionAtts, expressionAttNames] =
      await getUpdateExpressions(item, key, operation);
    const params = {
      TableName: tableName,
      Key: key,
      UpdateExpression: expression,
      ExpressionAttributeNames: expressionAttNames,
      ExpressionAttributeValues: expressionAtts,
    };
    return await dynamodb.update(params).promise();
  } catch (e) {
    console.error("Update Item Error: ", e, "\nUpdate params: ", params);
    throw "UpdateItemError";
  }
}

async function deleteItem(tableName, key) {
  let params;
  try {
    params = {
      TableName: tableName,
      Key: key,
    };
    return await dynamodb.delete(params).promise();
  } catch (e) {
    console.error("Delete Item Error: ", e, "\nDelete params: ", params);
    throw "DeleteItemError";
  }
}

async function queryWithPartitionKey(tableName, key) {
  let params;
  try {
    const [expression, expressionAtts] = await getQueryExpression(key);
    params = {
      TableName: tableName,
      KeyConditionExpression: expression,
      ExpressionAttributeValues: expressionAtts,
    };
    return await dynamodb.query(params).promise();
  } catch (e) {
    console.error(
      "Query Item With Partition key Error: ",
      e,
      "\nGet params: ",
      params
    );
    throw "QueryItemError";
  }
}

async function createOrUpdateDynamo(tableName, key, item) {
  const response = await getItem(tableName, key);
  if (get(response, "Item", null)) {
    await updateItem(tableName, key, item);
  } else {
    await putItem(tableName, item);
  }
}

async function getUpdateExpressions(params, key, operation) {
  let expression = `${operation} `;
  let expressionAtts = {};
  let expressionAttNames = {};
  Object.keys(key).forEach((k) => delete params[k]);
  if (operation === "SET") {
    Object.keys(params).forEach((p) => {
      expression += "#" + p + "=:" + p + ", ";
      expressionAtts[":" + p] = params[p];
      expressionAttNames["#" + p] = p;
    });
  } else {
    Object.keys(params).forEach((p) => {
      expression += "#" + p + " :" + p + ", ";
      expressionAtts[":" + p] = params[p];
      expressionAttNames["#" + p] = p;
    });
  }
  expression = expression.substring(0, expression.lastIndexOf(", "));
  return [expression, expressionAtts, expressionAttNames];
}

async function getQueryExpression(keys) {
  let expression = "";
  let expressionAtts = {};
  Object.keys(keys).forEach((k) => {
    expression += k + "=:" + k + " and ";
    expressionAtts[":" + k] = keys[k];
  });
  expression = expression.substring(0, expression.lastIndexOf(" and "));
  return [expression, expressionAtts];
}

async function queryWithIndex(tableName, index, keys, otherParams = null) {
  let params;
  try {
    const [expression, expressionAtts] = await getQueryExpression(keys);
    params = {
      TableName: tableName,
      IndexName: index,
      KeyConditionExpression: expression,
      ExpressionAttributeValues: expressionAtts,
    };
    if (otherParams) params = { ...params, ...otherParams };
    return await dynamodb.query(params).promise();
  } catch (e) {
    console.error("Query Item Error: ", e, "\nQuery params: ", params);
    throw "QueryItemError";
  }
}

module.exports = {
  getItem,
  putItem,
  updateItem,
  deleteItem,
  createOrUpdateDynamo,
  queryWithPartitionKey,
  queryWithIndex,
};
