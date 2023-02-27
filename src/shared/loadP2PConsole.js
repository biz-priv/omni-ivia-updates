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
  CONFIRMATION_COST_INDEX_KEY_NAME,
  IVIA_DDB,
  IVIA_VENDOR_ID,
  // IVIA_CARRIER_ID,
  STAGE,
} = process.env;
const IVIA_CARRIER_ID = "102";
const globalConsolIndex = "omni-ivia-ConsolNo-index-" + STAGE;
/**
 * point to point console // send console no
 */
const loadP2PConsole = async (dynamoData, shipmentAparData) => {
  console.log("loadP2PConsole");
  const CONSOL_NO = shipmentAparData.ConsolNo;

  const dataSet = await fetchDataFromTablesList(CONSOL_NO);
  console.log("dataSet", JSON.stringify(dataSet));

  const shipmentApar = dataSet.shipmentApar;
  const confirmationCost = dataSet.confirmationCost;
  const shipmentHeader = dataSet.shipmentHeader;
  const shipmentDesc = dataSet.shipmentDesc;

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

  const pStopTypeData = {
    stopType: "P",
    stopNum: 0,
    housebills: housebill_delimited,
    address: {
      address1: confirmationCost[0].ShipAddress1,
      city: confirmationCost[0].ShipCity,
      country: confirmationCost[0].FK_ShipCountry,
      state: confirmationCost[0].FK_ShipState,
      zip: confirmationCost[0].ShipZip,
    },
    companyName: confirmationCost[0].ShipName,
    cargo: cargo,
    scheduledDate: getGMTDiff(confirmationCost[0].PickupDateTime),
    specialInstructions:
      (confirmationCost[0].ShipAddress2 === ""
        ? ""
        : confirmationCost[0].ShipAddress2 + " ") +
      confirmationCost[0].PickupNote,
  };
  const dStopTypeData = {
    stopType: "D",
    stopNum: 1,
    housebills: housebill_delimited,
    address: {
      address1: confirmationCost[0].ConAddress1,
      city: confirmationCost[0].ConCity,
      country: confirmationCost[0].FK_ConCountry,
      state: confirmationCost[0].FK_ConState,
      zip: confirmationCost[0].ConZip,
    },
    companyName: confirmationCost[0].ConName,
    scheduledDate: getGMTDiff(confirmationCost[0].DeliveryDateTime),
    specialInstructions:
      (confirmationCost[0].ConAddress2 === ""
        ? ""
        : confirmationCost[0].ConAddress2 + " ") +
      confirmationCost[0].DeliveryNote,
  };

  const iviaPayload = {
    carrierId: IVIA_CARRIER_ID, //IVIA_CARRIER_ID = 1000025
    refNums: {
      refNum1: CONSOL_NO, // tbl_shipmentHeader.pk_orderNo as hwb/ tbl_confirmationCost.consolNo(if it is a consol)
      refNum2: "", // as query filenumber value is always ""
    },
    shipmentDetails: {
      stops: [pStopTypeData, dStopTypeData],
      dockHigh: "N", // req [Y / N]
      hazardous: getHazardous(shipmentDesc),
      liftGate: getLiftGate(shipmentApar),
      unNum: getUnNum(shipmentDesc), // accepts only 4 degit number as string
    },
  };
  console.log("iviaPayload", JSON.stringify(iviaPayload));

  const check = await validateAndCheckIfDataSentToIvia(iviaPayload, CONSOL_NO);
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
      ConsolNo: CONSOL_NO,
      FK_OrderNo: shipmentApar.map((e) => e.FK_OrderNo).join(","),
      payloadType: "loadP2PConsole",
      InsertedTimeStamp: momentTZ
        .tz("America/Chicago")
        .format("YYYY:MM:DD HH:mm:ss")
        .toString(),
      status: getStatus().IN_PROGRESS,
    };
    console.log("iviaTableData", iviaTableData);
    await putItem(IVIA_DDB, iviaTableData);
  }
};

async function fetchDataFromTablesList(CONSOL_NO) {
  try {
    /**
     * shipment apar
     */
    const sapparams = {
      TableName: SHIPMENT_APAR_TABLE,
      IndexName: globalConsolIndex,
      KeyConditionExpression: "ConsolNo = :ConsolNo",
      FilterExpression:
        "FK_VendorId = :FK_VendorId and Consolidation = :Consolidation",
      ExpressionAttributeValues: {
        ":ConsolNo": CONSOL_NO.toString(),
        ":FK_VendorId": IVIA_VENDOR_ID.toString(),
        ":Consolidation": "N",
      },
    };

    let shipmentApar = await ddb.query(sapparams).promise();
    shipmentApar = shipmentApar.Items;

    shipmentApar = shipmentApar.filter((e) =>
      ["HS", "TL"].includes(e.FK_ServiceId)
    );
    let confirmationCost = [],
      shipmentDesc = [],
      shipmentHeader = [];
    for (let index = 0; index < shipmentApar.length; index++) {
      const element = shipmentApar[index];
      /**
       * confirmationCost
       */
      const ccparams = {
        TableName: CONFIRMATION_COST,
        IndexName: CONFIRMATION_COST_INDEX_KEY_NAME,
        KeyConditionExpression: "FK_OrderNo = :FK_OrderNo",
        FilterExpression: "FK_SeqNo = :FK_SeqNo",
        ExpressionAttributeValues: {
          ":FK_OrderNo": element.FK_OrderNo.toString(),
          ":FK_SeqNo": element.SeqNo.toString(),
        },
      };
      let cc = await ddb.query(ccparams).promise();
      confirmationCost = [...confirmationCost, ...cc.Items];

      /**
       * shipmentHeader
       */
      if (element.SeqNo < 9999) {
        const shparams = {
          TableName: SHIPMENT_HEADER_TABLE,
          KeyConditionExpression: "PK_OrderNo = :PK_OrderNo",
          ExpressionAttributeValues: {
            ":PK_OrderNo": element.FK_OrderNo.toString(),
          },
        };
        let sh = await ddb.query(shparams).promise();
        shipmentHeader = [...shipmentHeader, ...sh.Items];
      }

      /**
       * shipmentDesc
       */
      const sdparams = {
        TableName: SHIPMENT_DESC_TABLE,
        KeyConditionExpression: "FK_OrderNo = :FK_OrderNo",
        ExpressionAttributeValues: {
          ":FK_OrderNo": element.FK_OrderNo.toString(),
        },
      };
      let sd = await ddb.query(sdparams).promise();
      shipmentDesc = [...shipmentDesc, ...sd.Items];
    }

    return {
      shipmentApar,
      confirmationCost,
      shipmentDesc,
      shipmentHeader,
    };
  } catch (error) {
    console.log("error", error);
    return {};
  }
}

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
    } catch (error) {
      console.log("payload validation error", error);
      resolve(true);
    }
    try {
      const params = {
        TableName: IVIA_DDB,
        IndexName: "omni-ivia-ConsolNo-index",
        KeyConditionExpression: "ConsolNo = :ConsolNo",
        FilterExpression: "status = :status",
        ExpressionAttributeValues: {
          ":ConsolNo": ConsolNo.toString(),
          ":status": getStatus().FAILED,
        },
      };
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
