const AWS = require("aws-sdk");
const { getLiftGate, getUnNum, validatePayload } = require("./dataHelper");
const moment = require("moment");
const momentTZ = require("moment-timezone");
const { v4: uuidv4 } = require("uuid");
const { queryWithPartitionKey, queryWithIndex } = require("./dynamo");
const ddb = new AWS.DynamoDB.DocumentClient({
  region: process.env.REGION,
});

const {
  SHIPMENT_APAR_TABLE, //"T19262"
  SHIPMENT_HEADER_TABLE,
  INSTRUCTIONS_TABLE,
  SHIPMENT_DESC_TABLE,
  CONFIRMATION_COST,
  CONSOL_STOP_HEADERS,
  CONSOL_STOP_ITEMS,
  CONFIRMATION_COST_INDEX_KEY_NAME,
  INSTRUCTIONS_INDEX_KEY_NAME,
  IVIA_DDB,
  IVIA_VENDOR_ID,
  IVIA_CARRIER_ID,
} = process.env;
/**
 * point to point console // send console no
 */
const loadP2PConsole = async (dynamoData, shipmentAparData) => {
  console.log("loadP2PConsole");

  const CONSOL_NO = shipmentAparData.ConsolNo;

  const tableList = getTablesAndPrimaryKey(dynamoData);
  const dataSet = await fetchDataFromTables(tableList, CONSOL_NO);
  console.log("dataSet", JSON.stringify(dataSet));

  const shipmentApar = dataSet.shipmentApar.filter(
    (e) => e.Consolidation === "N"
  );
  const confirmationCost = dataSet.confirmationCost;
  // if (confirmationCost.length === 1) {
  // }
  const shipmentHeader = dataSet.shipmentHeader;
  const shipmentDesc = dataSet.shipmentDesc.filter((e) =>
    shipmentApar.map((sa) => sa.FK_OrderNo).includes(e.FK_OrderNo)
  );

  const filteredConfirmationCost = confirmationCost.filter((e) => {
    const data = shipmentApar.filter(
      (sa) => sa.FK_OrderNo === e.FK_OrderNo && e.FK_SeqNo === sa.SeqNo
    );
    return data.length > 0;
  });
  console.log("filteredConfirmationCost", filteredConfirmationCost);

  const filteredOrderNoList = JSON.parse(
    JSON.stringify(filteredConfirmationCost)
  )
    .filter((e) => e.FK_SeqNo < 9999)
    .map((e) => e.FK_OrderNo);
  console.log("filteredOrderNoList***", filteredOrderNoList);

  const housebill_delimited = shipmentHeader
    .filter((e) => filteredOrderNoList.includes(e.PK_OrderNo))
    .map((e) => e.Housebill);

  const cargo = shipmentDesc.map((e) => {
    return {
      packageType:
        e.FK_PieceTypeId === "BOX"
          ? "BOX"
          : e.FK_PieceTypeId === "PLT"
          ? "PAL"
          : "PIE",
      quantity: e?.Pieces ?? "",
      length: e?.Length ? parseInt(e?.Length) : "",
      width: e?.Width ? parseInt(e?.Width) : "",
      height: e?.Height ? parseInt(e?.Height) : "",
      weight: e?.Weight ? parseInt(e?.Weight) : "",
      stackable: "Y", // hardcode
      turnable: "Y", // hardcode
    };
  });

  const pStopTypeData = JSON.parse(
    JSON.stringify(filteredConfirmationCost)
  ).map((e) => {
    return {
      stopType: "P",
      stopNum: 0,
      housebills: housebill_delimited,
      address: {
        address1: e.ShipAddress1,
        city: e.ShipCity,
        country: e.FK_ShipCountry,
        state: e.FK_ShipState,
        zip: e.ShipZip,
      },
      companyName: e.ShipName,
      cargo: cargo,
      scheduledDate: moment(e.PickupDateTime).diff("1970-01-01", "ms"), // ??
      specialInstructions:
        e.ShipAddress2 === "" ? "" : e.ShipAddress2 + " " + e.PickupNote,
    };
  });

  const dStopTypeData = JSON.parse(
    JSON.stringify(filteredConfirmationCost)
  ).map((e) => {
    return {
      stopType: "D",
      stopNum: 1,
      housebills: housebill_delimited,
      address: {
        address1: e.ConAddress1,
        city: e.ConCity,
        country: e.FK_ConCountry,
        state: e.FK_ConState,
        zip: e.ConZip,
      },
      companyName: e.ConName,
      scheduledDate: moment(e.DeliveryDateTime).diff("1970-01-01", "ms"), // ??
      specialInstructions:
        e.ConAddress2 === "" ? "" : e.ConAddress2 + " " + e.DeliveryNote,
    };
  });

  const iviaPayload = {
    carrierId: IVIA_CARRIER_ID, //IVIA_CARRIER_ID = 1000025
    refNums: {
      refNum1: CONSOL_NO, // tbl_shipmentHeader.pk_orderNo as hwb/ tbl_confirmationCost.consolNo(if it is a consol)
      refNum2: "", // as query filenumber value is always ""
    },
    shipmentDetails: {
      stops: [...pStopTypeData, ...dStopTypeData],
      dockHigh: "N", // req [Y / N]
      hazardous: shipmentDesc?.Hazmat ?? "N",
      liftGate: getLiftGate(shipmentApar?.ChargeCode ?? ""),
      unNum: getUnNum(shipmentDesc.shipmentDesc), // accepts only 4 degit number as string
    },
  };
  console.log("iviaPayload", JSON.stringify(iviaPayload));

  const check = await validateAndCheckIfDataSentToIvia(iviaPayload, CONSOL_NO);
  if (check) {
    //save to dynamo DB
    const iviaTableData = {
      id: uuidv4(),
      data: JSON.stringify(iviaPayload),
      Housebill: iviaPayload.shipmentDetails.stops[0].housebills.join(","),
      ConsolNo: CONSOL_NO,
      FK_OrderNo: shipmentApar.map((e) => e.FK_OrderNo).join(","),
      payloadType: "loadP2PConsole",
      InsertedTimeStamp: momentTZ
        .tz("America/Chicago")
        .format("YYYY:MM:DD HH:mm:ss")
        .toString(),
    };
    console.log("iviaTableData", iviaTableData);
    await putItem(IVIA_DDB, iviaTableData);
  }
};

/**
 * validate the payload structure and check from dynamodb if the data is sent to ivia priviously.
 * @param {*} payload
 * @param {*} ConsolNo
 * @returns
 */
function validateAndCheckIfDataSentToIvia(payload, ConsolNo) {
  return new Promise(async (resolve, reject) => {
    try {
      validatePayload(payload);
      const params = {
        TableName: IVIA_DDB,
        IndexName: "omni-ivia-ConsolNo-index",
        KeyConditionExpression: "ConsolNo = :ConsolNo",
        ExpressionAttributeValues: {
          ":ConsolNo": ConsolNo.toString(),
        },
      };
      const data = await ddb.query(params).promise();
      console.log("data", data.Items.length);
      if (data.Items.length > 0) {
        resolve(true);
      } else {
        resolve(false);
      }
    } catch (error) {
      console.log("dynamoError:", error);
      resolve(false);
    }
  });
}

/**
 * get table list and initial primary key and sort key name
 * @param {*} tableName
 * @param {*} dynamoData
 * @returns
 */
function getTablesAndPrimaryKey(tableName) {
  try {
    const tableList = {
      [SHIPMENT_APAR_TABLE]: {
        PK: "FK_OrderNo",
        SK: "SeqNo",
        sortName: "shipmentApar",
        indexKeyColumnName: "ConsolNo",
        indexKeyName: "omni-ivia-ConsolNo-index",
        type: "INDEX",
      },
      [INSTRUCTIONS_TABLE]: {
        PK: "PK_InstructionNo",
        SK: "",
        sortName: "shipmentInstructions",
        indexKeyColumnName: "ConsolNo",
        indexKeyName: "omni-ivia-ConsolNo-index",
        type: "INDEX",
      },
      [SHIPMENT_DESC_TABLE]: {
        PK: "FK_OrderNo",
        SK: "SeqNo",
        sortName: "shipmentDesc",
        indexKeyColumnName: "ConsolNo",
        indexKeyName: "omni-ivia-ConsolNo-index",
        type: "INDEX",
      },
      [CONFIRMATION_COST]: {
        PK: "PK_ConfirmationNo",
        SK: "FK_OrderNo",
        sortName: "confirmationCost",
        indexKeyColumnName: "ConsolNo",
        indexKeyName: "omni-ivia-ConsolNo-index",
        type: "INDEX",
      },
      [CONSOL_STOP_HEADERS]: {
        PK: "FK_OrderNo",
        SK: "FK_ConsolStopId",
        sortName: "consolStopHeaders",
        indexKeyColumnName: "FK_ConsolNo",
        indexKeyName: "omni-ivia-FK_ConsolNo-index",
        type: "INDEX",
      },
      // [CONSOL_STOP_ITEMS]: {
      //   PK: "FK_OrderNo",
      //   SK: "FK_ConsolStopId",
      //   sortName: "consolStopItems",
      //   indexKeyColumnName: "ConsolNo",
      //   indexKeyName: "omni-ivia-ConsolNo-index",
      //   type: "INDEX",
      // },
      // [SHIPMENT_HEADER_TABLE]: {
      //   PK: "PK_OrderNo",
      //   SK: "",
      //   sortName: "shipmentHeader",
      //   indexKeyColumnName: "ConsolNo",
      //   indexKeyName: "omni-ivia-ConsolNo-index",
      //   type: "INDEX",
      // },
    };

    return tableList;
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
async function fetchDataFromTables(tableList, CONSOL_NO) {
  try {
    const data = await Promise.all(
      Object.keys(tableList).map(async (e) => {
        const tableName = e;
        const ele = tableList[tableName];
        let data = [];
        if (tableName === SHIPMENT_APAR_TABLE) {
          data = await queryWithIndex(tableName, ele.indexKeyName, {
            [ele.indexKeyColumnName]: CONSOL_NO,
          });
          const params = {
            TableName: tableName,
            IndexName: ele.indexKeyName,
            KeyConditionExpression: "ConsolNo = :ConsolNo",
            FilterExpression: "FK_VendorId = :FK_VendorId",
            ExpressionAttributeValues: {
              ":ConsolNo": CONSOL_NO.toString(),
              ":FK_VendorId": IVIA_VENDOR_ID.toString(),
            },
          };
          console.log("params", params);
          data = await ddb.query(params).promise();
        } else {
          data = await queryWithIndex(tableName, ele.indexKeyName, {
            [ele.indexKeyColumnName]: CONSOL_NO.toString(),
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

    //fetch consolStopItems
    let consolStopItemsData = [];
    for (let index = 0; index < newObj.consolStopHeaders.length; index++) {
      const element = newObj.consolStopHeaders[index];
      const data = await queryWithIndex(
        CONSOL_STOP_ITEMS,
        "FK_ConsolStopId-index",
        {
          FK_ConsolStopId: element.PK_ConsolStopId,
        }
      );
      consolStopItemsData = [...consolStopItemsData, ...data.Items];
    }
    newObj["consolStopItems"] = consolStopItemsData;

    //fetch shipmentHeader
    let shipmentHeaderData = [];
    for (let index = 0; index < newObj.shipmentApar.length; index++) {
      const element = newObj.shipmentApar[index];
      const data = await queryWithPartitionKey(SHIPMENT_HEADER_TABLE, {
        PK_OrderNo: element.FK_OrderNo,
      });
      shipmentHeaderData = [...shipmentHeaderData, ...data.Items];
    }
    newObj["shipmentHeader"] = shipmentHeaderData;
    return newObj;
  } catch (error) {
    console.log("error:fetchDataFromTables", error);
  }
}

module.exports = { loadP2PConsole };

// {
//   "carrierId": 1000025, //required** hardcode  dev:- 1000025
//   "refNums": {
//     "refNum1": "244264", //ConfirmationCost[0].ConsolNo
//     "refNum2": "" // hardcode
//   },
//   "shipmentDetails": {
//     "stops": [
//       {
//         "stopType": "P", //required** hardcode P for pickup
//         "stopNum": 0,  //required** hardcode
//         "housebills": ["6958454"], // required** shipmentHeader.Housebill (1st we take FK_OrderNo from confirmationCost where FK_SeqNo < 9999 and then we filter the Housebill from shipmentHeader table based on orderNo)
//         "address": {
//           "address1": "1759 S linneman RD", // confirmationCost.ShipAddress1
//           "city": "Mt Prospect", // confirmationCost.ShipCity
//           "country": "US", // required** confirmationCost.FK_ShipCountry
//           "state": "IL", // confirmationCost.FK_ShipState
//           "zip": "60056" // required** confirmationCost.ShipZip
//         },
//         "companyName": "Omni Logistics", // confirmationCost.ShipName
//         "cargo": [ // all data from shipmentDesc condition (shipmentDesc.ConsolNo === shipmentApar.ConsolNo && shipmentApar.Consolidation === "N")
//           {
//             "packageType": "", //required** shipmentDesc.FK_PieceTypeId :- "BOX" = "BOX" , "PLT" = "PAL" , other any value "PIE"
//             "quantity": "1", //required** shipmentDesc.Pieces
//             "length": 68, // shipmentDesc.Length
//             "width": 48, // shipmentDesc.Width
//             "height": 46, // shipmentDesc.Height
//             "weight": 353, //required** shipmentDesc.Weight
//             "stackable": "Y", //required** hardcode
//             "turnable": "Y" //required** hardcode
//           }
//         ],
//         "scheduledDate": 1637913600000, //required** total time between confirmationCost.PickupDateTime and "1970-01-01" in "ms"
//         "specialInstructions": "" // confirmationCost.ShipAddress2 + confirmationCost.PickupNote
//       },
//       {
//         "stopType": "D", //required** hardcode D = delivery
//         "stopNum": 1, //required** hardcode
//         "housebills": ["6958454"], //required** same as P type
//         "address": {
//           "address1": "1414 Calconhook RD", // confirmationCost.ConAddress1
//           "city": "Sharon Hill", // confirmationCost.ConCity
//           "country": "US", // confirmationCost.FK_ConCountry
//           "state": "PA", // confirmationCost.FK_ConState
//           "zip": "19079" // confirmationCost.ConZip
//         },
//         "companyName": "Freight Force PHL", // confirmationCost.ConName
//         "scheduledDate": 1638176400000, //required** total time between confirmationCost.DeliveryDateTime and "1970-01-01" in "ms"
//         "specialInstructions": "" // confirmationCost.ConAddress2 + confirmationCost.DeliveryNote
//       }
//     ],
//     "dockHigh": "N", // required** [Y / N] default "N"
//     "hazardous": "N", // required**  shipmentDesc?.Hazmat
//     "liftGate": "N", // required** shipmentApar.ChargeCode
//     "unNum": "" // accepts only 4 degit number as string or empty string
//   }
// }
