const moment = require("moment-timezone");
const { deleteItem, createOrUpdateDynamo, updateItem } = require("./dynamo");

const mapCsvDataToJson = (data, mapArray) => {
  try {
    const parseData = JSON.parse(JSON.stringify(data));
    let newMap = {};
    mapArray.map((key) => {
      newMap[key] = parseData[key] ? parseData[key].toString() : "";
      if (key === "InsertedTimeStamp") {
        newMap["InsertedTimeStamp"] = moment
          .tz("America/Chicago")
          .format("YYYY:MM:DD HH:mm:ss")
          .toString();
      }
    });
    return newMap;
  } catch (error) {
    console.log("error:mapCsvDataToJson", error);
    throw error;
  }
};

function sortCommonItemsToSingleRow(itemList, primaryKey, uniqueFilterKey) {
  try {
    const grpupByPrimaryKey = itemList.reduce(function (rv, x) {
      (rv[x[primaryKey]] = rv[x[primaryKey]] || []).push(x);
      return rv;
    }, {});

    const sortedData = Object.keys(grpupByPrimaryKey).map((e, i) => {
      const obj = grpupByPrimaryKey[e];
      return obj.reduce((prev, current) =>
        +prev[uniqueFilterKey] > +current[uniqueFilterKey] ? prev : current
      );
    });
    return sortedData;
  } catch (error) {
    console.log("error:sortCommonItemsToSingleRow", error);
    throw error;
  }
}

function removeOperational(item, oprerationColumns) {
  try {
    oprerationColumns.map((e) => {
      delete item[e];
    });
    return item;
  } catch (error) {
    console.log("error:removeOperational", error);
    throw error;
  }
}

async function processData(
  tableName,
  primaryKey,
  sortKey,
  oprerationColumns,
  item
) {
  const operationType = item.Op;
  const mappedObj = removeOperational(item, oprerationColumns);
  const dbKey = {
    [primaryKey]: mappedObj[primaryKey],
    ...(sortKey != null ? { [sortKey]: mappedObj[sortKey] } : {}),
  };
  if (operationType === "D") {
    await deleteItem(tableName, dbKey);
  } else {
    /**
     * Edits an existing item's attributes, or adds a new item to the table
     * if it does not already exist by delegating to AWS.DynamoDB.updateItem().
     */
    await updateItem(tableName, dbKey, mappedObj);
  }
}

function prepareBatchFailureObj(data) {
  const batchItemFailures = data.map((e) => ({
    itemIdentifier: e.messageId,
  }));
  console.log("batchItemFailures", batchItemFailures);
  return { batchItemFailures };
}

module.exports = {
  mapCsvDataToJson,
  sortCommonItemsToSingleRow,
  processData,
  prepareBatchFailureObj,
};
