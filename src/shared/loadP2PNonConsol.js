const AWS = require("aws-sdk");
const {
  getLiftGate,
  getUnNum,
  validatePayload,
  getHazardous,
  getGMTDiff,
  getStatus,
} = require("./dataHelper");
const moment = require("moment");
const momentTZ = require("moment-timezone");
const { v4: uuidv4 } = require("uuid");
const { queryWithPartitionKey, queryWithIndex, putItem } = require("./dynamo");
const ddb = new AWS.DynamoDB.DocumentClient({
  region: process.env.REGION,
});

const {
  SHIPMENT_APAR_TABLE, //"T19262"
  SHIPMENT_HEADER_TABLE,
  SHIPMENT_DESC_TABLE,
  CONFIRMATION_COST,
  CONSOL_STOP_HEADERS,
  CONFIRMATION_COST_INDEX_KEY_NAME,
  IVIA_DDB,
  IVIA_VENDOR_ID,
  // IVIA_CARRIER_ID,
} = process.env;
const IVIA_CARRIER_ID = "102";
/**
 * non console p2p // send housebill no
 */
const loadP2PNonConsol = async (dynamoData, shipmentAparData) => {
  console.log("loadP2PNonConsol");

  //get the primary key and all table list
  const { tableList, primaryKeyValue } = getTablesAndPrimaryKey(
    dynamoData.dynamoTableName,
    dynamoData
  );

  //get data from all the requied tables
  const dataSet = await fetchDataFromTables(tableList, primaryKeyValue);
  console.log("dataSet", JSON.stringify(dataSet));

  const shipmentApar = shipmentAparData;
  const confirmationCost = dataSet.confirmationCost.filter(
    (e) => e.ConsolNo === "0"
  );
  const shipmentHeader = dataSet.shipmentHeader;

  const shipmentDesc = dataSet.shipmentDesc.filter((e) => e.ConsolNo === "0");

  const filteredConfirmationCost = confirmationCost.filter((e) => {
    return (
      e.FK_OrderNo === shipmentApar.FK_OrderNo &&
      e.FK_SeqNo === shipmentApar.SeqNo &&
      shipmentApar.Consolidation === "N"
    );
  });

  // exactly one shipfrom/ to address in tbl_confirmation_cost for file number
  if (filteredConfirmationCost.length > 1) {
    console.log("error: multiple line on confirmationCost");
    return {};
  }

  const housebill_delimited = shipmentHeader.map((e) => e.Housebill);

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
      scheduledDate: getGMTDiff(e.PickupDateTime),
      specialInstructions:
        (e.ShipAddress2 === "" ? "" : e.ShipAddress2 + " ") + e.PickupNote,
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
      scheduledDate: getGMTDiff(e.DeliveryDateTime),
      specialInstructions:
        (e.ConAddress2 === "" ? "" : e.ConAddress2 + " ") + e.DeliveryNote,
    };
  });

  const ORDER_NO_LIST = shipmentApar.FK_OrderNo;
  const filteredSH = shipmentDesc.filter((e) =>
    ORDER_NO_LIST.includes(e.FK_OrderNo)
  );

  const iviaPayload = {
    carrierId: IVIA_CARRIER_ID, // IVIA_CARRIER_ID = 1000025
    refNums: {
      refNum1: housebill_delimited[0] ?? "", // tbl_shipmentHeader.pk_orderNo as hwb/ tbl_confirmationCost.consolNo(if it is a consol)
      refNum2: confirmationCost[0].FK_OrderNo ?? "", // tbl_confirmationcost.fk_orderno as filenumber
    },
    shipmentDetails: {
      stops: [...pStopTypeData, ...dStopTypeData],
      dockHigh: "N", // req [Y / N]
      hazardous: getHazardous(filteredSH),
      liftGate: getLiftGate(shipmentApar),
      unNum: getUnNum(filteredSH), // accepts only 4 degit number as string
    },
  };
  console.log("iviaPayload", JSON.stringify(iviaPayload));

  const check = await validateAndCheckIfDataSentToIvia(
    iviaPayload,
    shipmentApar
  );
  if (!check) {
    //save to dynamo DB
    let houseBillList = [];
    iviaPayload.shipmentDetails.stops
      .filter((e) => e.stopType === "P")
      .map((e) => {
        houseBillList = [...houseBillList, ...e.housebills];
      });
    const iviaTableData = {
      id: uuidv4(),
      data: JSON.stringify(iviaPayload),
      Housebill: houseBillList.join(","),
      ConsolNo: shipmentAparData?.ConsolNo,
      FK_OrderNo: shipmentAparData?.FK_OrderNo,
      payloadType: "loadP2PNonConsol",
      InsertedTimeStamp: momentTZ
        .tz("America/Chicago")
        .format("YYYY:MM:DD HH:mm:ss")
        .toString(),
      status: getStatus().IN_PROGRESS,
    };
    console.log("iviaTableData", iviaTableData);
    await putItem(IVIA_DDB, iviaTableData);
  } else {
    console.log("Already sent to IVIA");
  }
};

function validateAndCheckIfDataSentToIvia(payload, shipmentApar) {
  return new Promise(async (resolve, reject) => {
    try {
      validatePayload(payload);
    } catch (error) {
      console.log("payload validation error", error);
      resolve(true);
    }
    try {
      const params = {
        TableName: IVIA_DDB,
        IndexName: "omni-ivia-ConsolNo-index",
        KeyConditionExpression: "ConsolNo = :ConsolNo",
        FilterExpression: "FK_OrderNo = :FK_OrderNo and status = :status",
        ExpressionAttributeValues: {
          ":ConsolNo": shipmentApar.ConsolNo.toString(),
          ":FK_OrderNo": shipmentApar.FK_OrderNo.toString(),
          ":status": getStatus().FAILED,
        },
      };
      console.log("params", params);
      const data = await ddb.query(params).promise();
      console.log("data", data.Items.length);
      if (data.Items.length > 0) {
        resolve(false);
      } else {
        resolve(true);
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

    return newObj;
  } catch (error) {
    console.log("error:fetchDataFromTables", error);
  }
}

module.exports = { loadP2PNonConsol };

// {
//   "carrierId": 1000025, //required** hardcode  dev:- 1000025
//   "refNums": {
//     "refNum1": "1234", //shipmentHeader.Housebill
//     "refNum2": "1234" // hardcode tbl_confirmationcost.fk_orderno
//   },
//   "shipmentDetails": {
//     "stops": [
//       {
//         "stopType": "P", //required** hardcode P for pickup
//         "stopNum": 0,  //required** hardcode
//         "housebills": ["6958454"], // required** all shipmentHeader.Housebill nos where shipmentHeader.ConsolNo === "0"
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
