const AWS = require("aws-sdk");
const {
  getLiftGate,
  getUnNum,
  validatePayload,
  getHazardous,
  getGMTDiff,
  getStatus,
  getNotesP2Pconsols,
} = require("./dataHelper");
const momentTZ = require("moment-timezone");
const { v4: uuidv4 } = require("uuid");
const { queryWithPartitionKey, queryWithIndex, putItem } = require("./dynamo");
const { sendSNSMessage } = require("./errorNotificationHelper");
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
const IVIA_CARRIER_ID = "102"; //NOTE:- for stage IVIA need to change it later
const globalConsolIndex = "omni-ivia-ConsolNo-index-" + STAGE;

const loadP2PConsole = async (dynamoData, shipmentAparData) => {
  console.log("load-P2P-Console");
  const CONSOL_NO = shipmentAparData.ConsolNo;

  /**
   * get data from all the requied tables
   * shipmentApar
   * confirmationCost
   * shipmentDesc
   * shipmentHeader
   */
  const dataSet = await fetchDataFromTablesList(CONSOL_NO);
  // console.log("dataSet", JSON.stringify(dataSet));

  const shipmentApar = dataSet.shipmentApar;
  const confirmationCost =
    dataSet.confirmationCost.length > 0 ? dataSet.confirmationCost[0] : {};
  const shipmentHeader = dataSet.shipmentHeader;
  const shipmentDesc = dataSet.shipmentDesc;

  //only used for liftgate
  const shipmentAparCargo = dataSet.shipmentAparCargo;

  //get all the housebill from shipmentHeader table
  const housebill_delimited = shipmentHeader.map((e) => e.Housebill);

  /**
   * preparing cargo obj form table shipmentDesc
   */
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

  /**
   * preparing pickup type stope obj from table ConfirmationCost
   */
  const pStopTypeData = {
    stopType: "P",
    stopNum: 0,
    housebills: housebill_delimited,
    address: {
      address1: confirmationCost?.ShipAddress1 ?? "",
      address2: confirmationCost?.ShipAddress2 ?? "",
      city: confirmationCost?.ShipCity ?? "",
      country: confirmationCost?.FK_ShipCountry ?? "",
      state: confirmationCost?.FK_ShipState ?? "",
      zip: confirmationCost?.ShipZip ?? "",
    },
    companyName: confirmationCost?.ShipName ?? "",
    cargo: cargo,
    scheduledDate: await getGMTDiff(
      confirmationCost?.PickupDateTime ?? "",
      confirmationCost?.ShipZip ?? "",
      confirmationCost?.FK_ShipCountry
    ),
    specialInstructions: (
      getNotesP2Pconsols(
        confirmationCost?.PickupTimeRange ?? "",
        confirmationCost?.PickupDateTime ?? "",
        "p"
      ) +
        "\r\n" +
        confirmationCost?.PickupNote ?? ""
    ).slice(0, 200),
  };

  /**
   * preparing delivery type stope obj from table ConfirmationCost
   */
  const dStopTypeData = {
    stopType: "D",
    stopNum: 1,
    housebills: housebill_delimited,
    address: {
      address1: confirmationCost?.ConAddress1 ?? "",
      address2: confirmationCost?.ConAddress2 ?? "",
      city: confirmationCost?.ConCity ?? "",
      country: confirmationCost?.FK_ConCountry ?? "",
      state: confirmationCost?.FK_ConState ?? "",
      zip: confirmationCost?.ConZip ?? "",
    },
    companyName: confirmationCost?.ConName ?? "",
    scheduledDate: await getGMTDiff(
      confirmationCost?.DeliveryDateTime ?? "",
      confirmationCost?.ConZip ?? "",
      confirmationCost?.FK_ConCountry
    ),
    specialInstructions: (
      getNotesP2Pconsols(
        confirmationCost?.DeliveryTimeRange,
        confirmationCost?.DeliveryDateTime,
        "d"
      ) +
      "\r\n" +
      confirmationCost?.DeliveryNote
    ).slice(0, 200),
  };

  //IVIA payload
  const iviaPayload = {
    carrierId: IVIA_CARRIER_ID, //IVIA_CARRIER_ID = dev 1000025 stage = 102
    refNums: {
      refNum1: CONSOL_NO, //shipmentApar.ConsolNo
      refNum2: "", // ignore
    },
    shipmentDetails: {
      stops: [pStopTypeData, dStopTypeData],
      dockHigh: "N", // req [Y / N]
      hazardous: getHazardous(shipmentDesc),
      liftGate: getLiftGate(shipmentAparCargo),
      unNum: getUnNum(shipmentDesc), // accepts only 4 degit number as string
    },
  };
  console.log("iviaPayload", JSON.stringify(iviaPayload));

  /**
   * validate the payload and check if it is already processed
   */
  const { check, errorMsg, isError } = await validateAndCheckIfDataSentToIvia(
    iviaPayload,
    CONSOL_NO
  );
  if (!check) {
    //save to dynamo DB
    let houseBillList = [];
    iviaPayload.shipmentDetails.stops
      .filter((e) => e.stopType === "P")
      .map((e) => {
        houseBillList = [...houseBillList, ...e.housebills];
      });

    //preparing obj for dynamoDB omni-ivia
    const iviaTableData = {
      id: uuidv4(),
      data: JSON.stringify(iviaPayload),
      Housebill: houseBillList.join(","),
      ConsolNo: CONSOL_NO,
      FK_OrderNo: shipmentApar.map((e) => e.FK_OrderNo).join(","),
      payloadType: "P2PConsole",
      InsertedTimeStamp: momentTZ
        .tz("America/Chicago")
        .format("YYYY:MM:DD HH:mm:ss")
        .toString(),
      status: isError ? getStatus().FAILED : getStatus().IN_PROGRESS,
      errorMsg: isError ? JSON.stringify(errorMsg) : "",
      errorReason: isError ? "validation error" : "",
    };
    console.log("iviaTableData", iviaTableData);
    await putItem(IVIA_DDB, iviaTableData);
    if (isError) {
      await sendSNSMessage(iviaTableData);
    }
  }
};

/**
 * fetch data from the tables
 * @param {*} tableList
 * @param {*} primaryKeyValue
 * @returns
 */
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
        "FK_VendorId = :FK_VendorId and Consolidation = :Consolidation and SeqNo <> :SeqNo and FK_OrderNo <> :FK_OrderNo",
      ExpressionAttributeValues: {
        ":ConsolNo": CONSOL_NO.toString(),
        ":FK_VendorId": IVIA_VENDOR_ID.toString(),
        ":Consolidation": "N",
        ":SeqNo": "9999",
        ":FK_OrderNo": CONSOL_NO.toString(),
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

    /**
     * Fetch shipment apar for liftgate based on shipmentDesc.FK_OrderNo
     */
    const FK_OrderNoList = [...new Set(shipmentDesc.map((e) => e.FK_OrderNo))];
    console.log("FK_OrderNoList for cargo", FK_OrderNoList);

    let shipmentAparCargo = [];
    for (let index = 0; index < FK_OrderNoList.length; index++) {
      const FK_OrderNo = FK_OrderNoList[index];
      const sapcParams = {
        TableName: SHIPMENT_APAR_TABLE,
        KeyConditionExpression: "FK_OrderNo = :FK_OrderNo",
        ExpressionAttributeValues: {
          ":FK_OrderNo": FK_OrderNo.toString(),
        },
      };

      let sac = await ddb.query(sapcParams).promise();
      shipmentAparCargo = [...shipmentAparCargo, ...sac.Items];
    }

    return {
      shipmentApar,
      confirmationCost,
      shipmentDesc,
      shipmentHeader,
      shipmentAparCargo,
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
 * @returns 3 variables
 *  1> check :- true/false  if omni-ivia don't have record with success status then false else true
 *  2> isError: true/false if we have validation then true else false
 *  3> errorMsg: "" if isErroris true then this variable will contain the validation error msg
 */
function validateAndCheckIfDataSentToIvia(payload, ConsolNo) {
  return new Promise(async (resolve, reject) => {
    let errorMsg = validatePayload(payload);
    console.log("errorMsg", errorMsg);

    try {
      //fetch from ivia table and check if data processed or not
      const params = {
        TableName: IVIA_DDB,
        IndexName: "omni-ivia-ConsolNo-index",
        KeyConditionExpression: "ConsolNo = :ConsolNo",
        ExpressionAttributeValues: {
          ":ConsolNo": ConsolNo.toString(),
        },
      };
      console.log("params", params);
      const data = await ddb.query(params).promise();
      console.log("data:ivia", data);

      if (data.Items.length > 0) {
        //check if payload is processed or or in progress
        const latestData = data.Items.filter(
          (e) =>
            e.status === getStatus().SUCCESS ||
            e.status === getStatus().IN_PROGRESS
        );
        if (latestData.length > 0) {
          resolve({ check: true, errorMsg: "", isError: false });
        } else {
          //check if the latest failed payload is same with upcomming payload or not.
          let errorObj = data.Items.filter(
            (e) => e.status === getStatus().FAILED
          );
          errorObj = errorObj.sort(function (x, y) {
            return x.InsertedTimeStamp < y.InsertedTimeStamp ? 1 : -1;
          })[0];

          //checking if the latest table payload is same with prepared payload
          if (errorObj.data != JSON.stringify(payload)) {
            //check for if we have validation error
            if (errorMsg != "") {
              resolve({ check: false, errorMsg: errorMsg, isError: true });
            } else {
              resolve({ check: false, errorMsg: "", isError: false });
            }
          } else {
            resolve({ check: true, errorMsg: "", isError: false });
          }
        }
      } else {
        if (errorMsg != "") {
          resolve({ check: false, errorMsg: errorMsg, isError: true });
        } else {
          resolve({ check: false, errorMsg: "", isError: false });
        }
      }
    } catch (error) {
      console.log("dynamoError:", error);
      resolve({ check: false, errorMsg: "", isError: false });
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
