const AWS = require("aws-sdk");
const { v4: uuidv4 } = require("uuid");
const momentTZ = require("moment-timezone");
const { prepareBatchFailureObj } = require("../shared/dataHelper");
const {
  queryWithPartitionKey,
  queryWithIndex,
  putItem,
} = require("../shared/dynamo");
const { loadP2PNonConsol } = require("../shared/loadP2PNonConsol");
const { loadP2PConsole } = require("../shared/loadP2PConsole");
const { loadMultistopConsole } = require("../shared/loadMultistopConsole");

const {
  SHIPMENT_APAR_TABLE, //"T19262"
  SHIPMENT_HEADER_TABLE,
  // CONSIGNEE_TABLE,
  // SHIPPER_TABLE,
  INSTRUCTIONS_TABLE,
  SHIPMENT_DESC_TABLE,

  CONFIRMATION_COST,
  CONSOL_STOP_HEADERS,
  CONSOL_STOP_ITEMS,
  CONFIRMATION_COST_INDEX_KEY_NAME,
  INSTRUCTIONS_INDEX_KEY_NAME,
  IVIA_DDB,
  IVIA_VENDOR_ID,
  // IVIA_CARRIER_ID,
} = process.env;

module.exports.handler = async (event, context, callback) => {
  let sqsEventRecords = [];
  try {
    console.log("event", JSON.stringify(event));
    sqsEventRecords = event.Records;

    const faildSqsItemList = [];

    for (let index = 0; index < sqsEventRecords.length; index++) {
      try {
        const sqsItem = sqsEventRecords[index];
        const dynamoData = JSON.parse(sqsItem.body);

        //get the primary key and all table list
        const { tableList, primaryKeyValue } = getTablesAndPrimaryKey(
          dynamoData.dynamoTableName,
          dynamoData
        );

        //get data from all the requied tables
        const dataSet = await fetchDataFromTables(tableList, primaryKeyValue);
        console.log("dataSet", JSON.stringify(dataSet));

        const shipmentAparSortedData = dataSet.shipmentApar
          .filter((e) => e.FK_VendorId === IVIA_VENDOR_ID)
          .reduce((a, b) => {
            return a.SeqNo > b.SeqNo ? a : b;
          });

        //if got multiple data , take latest one based on seqNo and time.
        const shipmentAparData =
          dynamoData.dynamoTableName === SHIPMENT_APAR_TABLE
            ? AWS.DynamoDB.Converter.unmarshall(dynamoData.NewImage)
            : shipmentAparSortedData;
        // console.log("shipmentAparData", shipmentAparData);

        if (shipmentAparData?.FK_VendorId != IVIA_VENDOR_ID) {
          continue;
        }

        let iviaPayload = "";

        if (["HS", "TL"].includes(shipmentAparData?.FK_ServiceId)) {
          if (dataSet.confirmationCost?.[0]?.ConsolNo === "0") {
            // payload 2 way starts // non consol p2p // send housebill no
            // exactly one shipfrom/ to address in tbl_confirmation_cost for file number
            if (dataSet.confirmationCost.length === 1) {
              //payload 2
              iviaPayload = loadP2PNonConsol(dataSet, shipmentAparData);
            } else {
              //exception payload 2
              console.log("exception payload 2");
              throw "exception payload 2";
            }
          } else {
            //one ship from/ to address in tbl_confiramation_cost for consol number
            if (
              dataSet.confirmationCost[0].ConsolNo > 0 &&
              dataSet.confirmationCost.length === 1
            ) {
              // payload 3 way starts // send consol_no  //   p2p consol
              // pieces weight dims for each housebill in consol
              iviaPayload = loadP2PConsole(dataSet, shipmentAparData);
            } else {
              //exception
              console.log("exception payload 2");
              throw "exception payload 3";
            }
          }
        } else if (shipmentAparData.FK_ServiceId === "MT") {
          // payload 1 way starts // send consol_no // multistop consol
          // exectly one pickup and one delivery address in consol stop headers for each housebill
          if (dataSet.consolStopHeaders.length === 1) {
            iviaPayload = loadMultistopConsole(dataSet, shipmentAparData);
          } else {
            //exception
            console.log("exception payload 1");
            throw "exception payload 1";
          }
        } else {
          //exception
          console.log("exception ");
          throw "exception";
        }

        //prepare the payload
        console.log("iviaObj", JSON.stringify(iviaPayload));

        //save to dynamo DB
        const iviaTableData = {
          id: uuidv4(),
          data: JSON.stringify(iviaPayload),
          Housebill: iviaPayload.shipmentDetails.stops[0].housebills.join(","),
          ConsolNo: shipmentAparData?.ConsolNo,
          FK_OrderNo: shipmentAparData?.FK_OrderNo,
          InsertedTimeStamp: momentTZ
            .tz("America/Chicago")
            .format("YYYY:MM:DD HH:mm:ss")
            .toString(),
        };
        console.log(iviaTableData, JSON.stringify(iviaTableData));
        await putItem(IVIA_DDB, iviaTableData);
      } catch (error) {
        console.log("error", error);
      }
    }
    return prepareBatchFailureObj(faildSqsItemList);
  } catch (error) {
    console.error("Error", error);
    return prepareBatchFailureObj(sqsEventRecords);
  }
};

/**
 * get table list and initial primary key and sort key name
 * @param {*} tableName
 * @param {*} dynamoData
 * @returns
 */
function getTablesAndPrimaryKey(tableName, dynamoData) {
  try {
    const tableList = {
      [SHIPMENT_APAR_TABLE]: {
        PK: "FK_OrderNo",
        SK: "SeqNo",
        sortName: "shipmentApar",
        type: "PRIMARY_KEY",
      },
      [SHIPMENT_HEADER_TABLE]: {
        PK: "PK_OrderNo",
        SK: "",
        sortName: "shipmentHeader",
        type: "PRIMARY_KEY",
      },
      // [CONSIGNEE_TABLE]: {
      //   PK: "FK_ConOrderNo",
      //   SK: "",
      //   sortName: "consignee",
      //   type: "PRIMARY_KEY",
      // },
      // [SHIPPER_TABLE]: {
      //   PK: "FK_ShipOrderNo",
      //   SK: "",
      //   sortName: "shipper",
      //   type: "PRIMARY_KEY",
      // },
      [INSTRUCTIONS_TABLE]: {
        PK: "PK_InstructionNo",
        SK: "",
        sortName: "shipmentInstructions",
        indexKeyColumnName: "FK_OrderNo",
        indexKeyName: INSTRUCTIONS_INDEX_KEY_NAME, //"omni-wt-instructions-orderNo-index-{stage}"
        type: "INDEX",
      },
      [SHIPMENT_DESC_TABLE]: {
        PK: "FK_OrderNo",
        SK: "SeqNo",
        sortName: "shipmentDesc",
        type: "PRIMARY_KEY",
      },
      [CONFIRMATION_COST]: {
        PK: "PK_ConfirmationNo",
        SK: "FK_OrderNo",
        sortName: "confirmationCost",
        indexKeyColumnName: "FK_OrderNo",
        indexKeyName: CONFIRMATION_COST_INDEX_KEY_NAME, //omni-wt-confirmation-cost-orderNo-index-{stage}
        type: "INDEX",
      },
      [CONSOL_STOP_ITEMS]: {
        PK: "FK_OrderNo",
        SK: "FK_ConsolStopId",
        sortName: "consolStopItems",
        type: "PRIMARY_KEY",
      },
    };

    let data = "";
    let primaryKeyValue = "";

    if (tableName === CONSOL_STOP_HEADERS) {
      data = tableList[tableName];
      primaryKeyValue = "FK_OrderNo";
    } else {
      data = tableList[tableName];
      primaryKeyValue =
        data.type === "INDEX"
          ? dynamoData.NewImage[data.indexKeyColumnName].S
          : dynamoData.Keys[data.PK].S;
    }

    return { tableList, primaryKeyValue };
  } catch (error) {
    console.info("error:unable to select table", error);
    console.info("tableName", tableName);
    throw error;
  }
}

/**
 * fetch data from the tables
 * @param {*} tableList
 * @param {*} primaryKeyValue
 * @returns
 */
async function fetchDataFromTables(tableList, primaryKeyValue) {
  try {
    const data = await Promise.all(
      Object.keys(tableList).map(async (e) => {
        const tableName = e;
        const ele = tableList[tableName];
        let data = [];

        if (ele.type === "INDEX") {
          // console.log(tableName, ele);
          data = await queryWithIndex(tableName, ele.indexKeyName, {
            [ele.indexKeyColumnName]: primaryKeyValue,
          });
        } else {
          data = await queryWithPartitionKey(tableName, {
            [ele.PK]: primaryKeyValue,
          });
        }

        return { [ele.sortName]: data.Items };
      })
    );
    const newObj = {};
    data.map((e) => {
      const objKey = Object.keys(e)[0];
      newObj[objKey] = e[objKey];
    });

    //fetch consolStopHeaders
    let consolStopHeaderData = [];
    for (let index = 0; index < newObj.consolStopItems.length; index++) {
      const element = newObj.consolStopItems[index];
      const data = await queryWithPartitionKey(CONSOL_STOP_HEADERS, {
        PK_ConsolStopId: element.FK_ConsolStopId,
      });
      consolStopHeaderData = [...consolStopHeaderData, ...data.Items];
    }
    newObj["consolStopHeaders"] = consolStopHeaderData;
    return newObj;
  } catch (error) {
    console.log("error:fetchDataFromTables", error);
  }
}
