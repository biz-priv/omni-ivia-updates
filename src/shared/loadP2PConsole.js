const AWS = require("aws-sdk");
const {
  getLiftGate,
  getUnNum,
  validatePayload,
  getHazardous,
  getGMTDiff,
  getStatus,
  getNotesP2Pconsols,
  checkAddressByGoogleApi,
  checkIfShipmentHeaderOrderDatePass,
} = require("./dataHelper");
const momentTZ = require("moment-timezone");
const { v4: uuidv4 } = require("uuid");
const { queryWithPartitionKey, queryWithIndex, putItem } = require("./dynamo");
const { sendSNSMessage } = require("./errorNotificationHelper");
const { get } = require("lodash");
const ddb = new AWS.DynamoDB.DocumentClient({
  region: process.env.REGION,
});

const {
  SHIPMENT_APAR_TABLE, //"T19262"
  SHIPMENT_HEADER_TABLE,
  SHIPMENT_DESC_TABLE,
  CONSOL_STOP_HEADERS,
  CONSOL_STOP_ITEMS,
  CONFIRMATION_COST,
  CUSTOMER_TABLE,
  CONFIRMATION_COST_INDEX_KEY_NAME,
  IVIA_DDB,
  IVIA_VENDOR_ID,
  INSTRUCTIONS_TABLE,
  INSTRUCTIONS_INDEX_KEY_NAME,
  EQUIPMENT_TABLE,
  IVIA_CARRIER_ID,
  STAGE,
} = process.env;
// const IVIA_CARRIER_ID = "102"; //NOTE:- for stage IVIA need to change it later
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
  const shipmentInstructions = dataSet.shipmentInstructions;
  const equipment = dataSet.equipment.length > 0 ? dataSet.equipment[0] : {};
  const customer = dataSet.customer.length > 0 ? dataSet.customer[0] : {};
  const consolStopHeaders = dataSet.consolStopHeaders.length > 0 ? dataSet.consolStopHeaders[0] : {};
  console.log("console stop header", consolStopHeaders)

  /**
   * we need to check in the shipmentHeader.OrderDate >= '2023:04:01 00:00:00' - for both nonconsol and consol -> if this condition satisfies, we send the event to Ivia, else we ignore
   * Ignore the event if there is no OrderDate or it is "1900
   */
  if (!checkIfShipmentHeaderOrderDatePass(shipmentHeader)) {
    console.log(
      "event IGNORED shipmentHeader.OrderDate LESS THAN 2023:04:01 00:00:00 "
    );
    return {};
  }

  //only used for liftgate
  const shipmentAparCargo = dataSet.shipmentAparCargo;

  //get all the housebill from shipmentHeader table
  const housebill_delimited = shipmentHeader.map((e) => e.Housebill);

  /**
   * preparing cargo obj form table shipmentDesc based on shipmentAPAR.FK_OrderNo
   */
  const cargo = shipmentDesc
    .map((e) => {
      const checkIfZero =
        parseInt(e?.Length != "" ? e?.Length : 0) +
          parseInt(e?.Width != "" ? e?.Width : 0) +
          parseInt(e?.Height != "" ? e?.Height : 0) ===
          0
          ? true
          : false;
      return {
        packageType:
          e.FK_PieceTypeId === "BOX"
            ? "BOX"
            : e.FK_PieceTypeId === "PLT"
              ? "PAL"
              : "PIE",
        quantity: e?.Pieces ?? "",
        length: checkIfZero ? 1 : parseInt(e?.Length),
        width: checkIfZero ? 1 : parseInt(e?.Width),
        height: checkIfZero ? 1 : parseInt(e?.Height),
        weight: e?.Weight ? parseInt(e?.Weight) : "",
        stackable: "Y", // hardcode
        turnable: "Y", // hardcode
      };
    })
    .filter((e) => e.quantity != "" && e.quantity != 0 && e.quantity != "0");
  const FK_OrderNoListForIns = [
    ...new Set(shipmentApar.map((e) => e.FK_OrderNo)),
  ];

  //fetch notes from Instructions table based on shipment_apar table FK_OrderNo data
  // getting pickup type notes based on Type == "P"
  const pInsNotes = shipmentInstructions
    .filter(
      (si) =>
        si.Type.toUpperCase() === "P" &&
        FK_OrderNoListForIns.includes(si.FK_OrderNo)
    )
    .map((ei) => ei.Note)
    .join(" ");

  // getting delivery type notes based on Type == "D"
  const dInsNotes = shipmentInstructions
    .filter(
      (si) =>
        si.Type.toUpperCase() === "D" &&
        FK_OrderNoListForIns.includes(si.FK_OrderNo)
    )
    .map((ei) => ei.Note)
    .join(" ");

  /**
   * preparing pickup type stope obj from table ConfirmationCost
   * based on shipmentAPAR.FK_OrderNo and shipmentAPAR.FK_SeqNo
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
    scheduledDate: "",
    specialInstructions: (
      getNotesP2Pconsols(
        confirmationCost?.PickupTimeRange ?? "",
        confirmationCost?.PickupDateTime ?? "",
        "p"
      ) +
      "\r\n" +
      (confirmationCost?.ShipContact.length > 0 ||
        confirmationCost?.ShipPhone.length > 0
        ? "Contact " +
        confirmationCost?.ShipContact +
        " " +
        confirmationCost?.ShipPhone +
        "\r\n"
        : "")
    ).slice(0, 200),
    cutoffDate: "",
  };

  const ptypeAddressData = await checkAddressByGoogleApi(pStopTypeData.address);
  pStopTypeData.address = ptypeAddressData;
  pStopTypeData.scheduledDate = await getGMTDiff(
    confirmationCost?.PickupDateTime ?? "",
    ptypeAddressData
  );

  // const pickUpcutoffTime = get(csh, "ConsolStopTimeBegin", "")
  // const pickUpCutoffDate = get(csh, "ConsolStopDate", "")
  // if (pickUpcutoffTime && pickUpCutoffDate && pickUpcutoffTime.length > 11 && pickUpCutoffDate.length > 0) {
  //   if (pickUpcutoffTime.slice(11) != "00:00:00.000") {
  //     pcutoffVal = pickUpCutoffDate.slice(0, 11) + pickUpcutoffTime.slice(11)
  //   } else {
  //     pcutoffVal = null
  //   }
  // } else {
  //   pcutoffVal = null
  // }
  const pickupcutoffTimeRange = get(confirmationCost, "PickupTimeRange", "")
  if (pickupcutoffTimeRange) {
    if (pickupcutoffTimeRange.slice(11) != "00:00:00.000") {
      pStopTypeData.cutoffDate = await getGMTDiff(
        pickupcutoffTimeRange,
        ptypeAddressData
      );
    } else {
      pStopTypeData.cutoffDate = null
    }
  } else {
    pStopTypeData.cutoffDate = null
  }
  /**
   * preparing delivery type stope obj from table ConfirmationCost
   * based on shipmentAPAR.FK_OrderNo and shipmentAPAR.FK_SeqNo
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
    scheduledDate: "",
    specialInstructions: (
      getNotesP2Pconsols(
        confirmationCost?.DeliveryTimeRange,
        confirmationCost?.DeliveryDateTime,
        "d"
      ) +
      "\r\n" +
      (confirmationCost?.ConContact.length > 0 ||
        confirmationCost?.ConPhone.length > 0
        ? "Contact " +
        confirmationCost?.ConContact +
        " " +
        confirmationCost?.ConPhone +
        "\r\n"
        : "") +
      (confirmationCost?.DeliveryNote ?? "")
    ).slice(0, 200),
    cutoffDate: "",
  };
  const dtypeAddressData = await checkAddressByGoogleApi(dStopTypeData.address);
  dStopTypeData.address = dtypeAddressData;
  dStopTypeData.scheduledDate = await getGMTDiff(
    confirmationCost?.DeliveryDateTime ?? "",
    dtypeAddressData
  );
  // if (shipmentHeader[0].ScheduledBy == "T") {
  //   dStopTypeData.cutoffDate = await getGMTDiff(
  //     shipmentHeader[0].ScheduledDateTimeRange,
  //     dtypeAddressData
  //   );
  // }
  // else {
  //   dStopTypeData.cutoffDate = null;
  // }


  const deliverycutoffTimeRange = get(confirmationCost, "DeliveryTimeRange", "")
  if (deliverycutoffTimeRange) {
    if (deliverycutoffTimeRange.slice(11) != "00:00:00.000") {
      dStopTypeData.cutoffDate = await getGMTDiff(
        deliverycutoffTimeRange,
        dtypeAddressData
      );
    } else {
      dStopTypeData.cutoffDate = null
    }
  } else {
    dStopTypeData.cutoffDate = null
  }

  const total = shipmentApar[0].Total

  //IVIA payload
  const iviaPayload = {
    carrierId: IVIA_CARRIER_ID, //IVIA_CARRIER_ID = dev 1000025 stage = 102
    refNums: {
      refNum1: CONSOL_NO, //shipmentApar.ConsolNo
      refNum2: customer?.CustName?.slice(0, 19) ?? "", // ignore
      refNum3: get(shipmentHeader[0], "HandlingStation", ""), //HandlingStation
    },
    shipmentDetails: {
      stops: [pStopTypeData, dStopTypeData],
      dockHigh: "N", // req [Y / N]
      hazardous: getHazardous(shipmentDesc),
      liftGate: getLiftGate(shipmentAparCargo, shipmentHeader),
      unNum: getUnNum(shipmentDesc), // accepts only 4 degit number as string
      notes: equipment?.Description ?? "",
      revenue: +parseFloat(total).toFixed(2) ?? "",
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

    if (isError) {
      await sendSNSMessage(iviaTableData);
    }
    console.log("iviaTableData", iviaTableData);
    await putItem(IVIA_DDB, iviaTableData);
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
      shipmentHeader = [],
      shipmentInstructions = [];
    let consolStopHeaders = [];
    let consolStopItems = [];
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

      /**
       * shipmentInstructions
       */
      const iparams = {
        TableName: INSTRUCTIONS_TABLE,
        IndexName: INSTRUCTIONS_INDEX_KEY_NAME,
        KeyConditionExpression: "FK_OrderNo = :FK_OrderNo",
        ExpressionAttributeValues: {
          ":FK_OrderNo": element.FK_OrderNo.toString(),
        },
      };
      let ins = await ddb.query(iparams).promise();
      shipmentInstructions = [...shipmentInstructions, ...ins.Items];

      /**
      * consolStopItems
      */

      const cstparams = {
        TableName: CONSOL_STOP_ITEMS,
        KeyConditionExpression: "FK_OrderNo = :FK_OrderNo",
        ExpressionAttributeValues: {
          ":FK_OrderNo": element.FK_OrderNo.toString(),
        },
      };
      let cst = await ddb.query(cstparams).promise();
      consolStopItems = [...consolStopItems, ...cst.Items];

      /**
       * consolStopHeader
       */

      for (let index = 0; index < consolStopItems.length; index++) {
        const csitem = consolStopItems[index];
        const cshparams = {
          TableName: CONSOL_STOP_HEADERS,
          KeyConditionExpression: "PK_ConsolStopId = :PK_ConsolStopId",
          FilterExpression: "FK_ConsolNo = :ConsolNo",
          ExpressionAttributeValues: {
            ":PK_ConsolStopId": csitem.FK_ConsolStopId.toString(),
            ":ConsolNo": CONSOL_NO.toString(),
          },
        };
        let csh = await ddb.query(cshparams).promise();
        consolStopHeaders = [...consolStopHeaders, ...csh.Items];
      }
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

    /**
     * EQUIPMENT_TABLE
     */
    const shAparForEQParam = {
      TableName: SHIPMENT_APAR_TABLE,
      IndexName: globalConsolIndex,
      KeyConditionExpression: "ConsolNo = :ConsolNo",
      FilterExpression:
        "FK_VendorId = :FK_VendorId and FK_OrderNo = :FK_OrderNo",
      ExpressionAttributeValues: {
        ":ConsolNo": CONSOL_NO.toString(),
        ":FK_VendorId": IVIA_VENDOR_ID.toString(),
        ":FK_OrderNo": CONSOL_NO.toString(),
      },
    };

    let shAparForEQData = await ddb.query(shAparForEQParam).promise();
    shAparForEQData = shAparForEQData.Items;
    console.log("shAparForEQData", shAparForEQData);

    let equipment = [];
    if (shAparForEQData.length > 0 && shAparForEQData[0].FK_EquipmentCode) {
      const FK_EquipmentCode = shAparForEQData[0].FK_EquipmentCode;
      const equipmentParam = {
        TableName: EQUIPMENT_TABLE,
        KeyConditionExpression: "PK_EquipmentCode = :PK_EquipmentCode",
        ExpressionAttributeValues: {
          ":PK_EquipmentCode": FK_EquipmentCode.toString(),
        },
      };

      const eqData = await ddb.query(equipmentParam).promise();
      equipment = eqData.Items;
      console.log("equipment", eqData);
    }

    /**
    * CUSTOMER_TABLE
    */
    let customer = []
    if (
      shipmentHeader.length > 0 &&
      shipmentHeader[0].BillNo != ""
    ) {
      const customerParam = {
        TableName: CUSTOMER_TABLE,
        KeyConditionExpression: "PK_CustNo = :PK_CustNo",
        ExpressionAttributeValues: {
          ":PK_CustNo": shipmentHeader[0].BillNo,
        },
      };

      customer = await ddb.query(customerParam).promise();
      customer = customer.Items;
    }




    return {
      shipmentApar,
      confirmationCost,
      shipmentDesc,
      shipmentHeader,
      shipmentAparCargo,
      shipmentInstructions,
      equipment,
      customer,
      consolStopHeaders
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
        IndexName: "omni-ivia-ConsolNo-FK_OrderNo-index",
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
          if (
            errorObj.hasOwnProperty("data") &&
            errorObj.data != JSON.stringify(payload)
          ) {
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

// every field is required only refNum2, specialInstructions may be empty
// {
//   "carrierId": 1000025, // hardcode  dev:- 1000025 stage:- 102
//   "refNums": {
//     "refNum1": "244264", //shipmentApar.ConsolNo
//     "refNum2": "" // hardcode
//   },
//   "shipmentDetails": {
//     "stops": [
//       {
//         "stopType": "P", // hardcode P for pickup
//         "stopNum": 0,  // hardcode
//         "housebills": ["6958454"], //  shipmentHeader.Housebill (1st we take FK_OrderNo from confirmationCost where FK_SeqNo < 9999 and then we filter the Housebill from shipmentHeader table based on orderNo)
//         "address": {
//           "address1": "1759 S linneman RD", // confirmationCost.ShipAddress1
//           "city": "Mt Prospect", // confirmationCost.ShipCity
//           "country": "US", //  confirmationCost.FK_ShipCountry
//           "state": "IL", // confirmationCost.FK_ShipState
//           "zip": "60056" //  confirmationCost.ShipZip
//         },
//         "companyName": "Omni Logistics", // confirmationCost.ShipName
//         "cargo": [ // all data from shipmentDesc condition (shipmentDesc.ConsolNo === shipmentApar.ConsolNo && shipmentApar.Consolidation === "N")
//           {
//             "packageType": "", // shipmentDesc.FK_PieceTypeId :- "BOX" = "BOX" , "PLT" = "PAL" , other any value "PIE"
//             "quantity": "1", // shipmentDesc.Pieces
//             "length": 68, // shipmentDesc.Length
//             "width": 48, // shipmentDesc.Width
//             "height": 46, // shipmentDesc.Height
//             "weight": 353, // shipmentDesc.Weight
//             "stackable": "Y", // hardcode
//             "turnable": "Y" // hardcode
//           }
//         ],
//         "scheduledDate": 1637913600000, // check from code
//         "specialInstructions": "" // check from code
//       },
//       {
//         "stopType": "D", // hardcode D = delivery
//         "stopNum": 1, // hardcode
//         "housebills": ["6958454"], // same as P type
//         "address": {
//           "address1": "1414 Calconhook RD", // confirmationCost.ConAddress1
//           "city": "Sharon Hill", // confirmationCost.ConCity
//           "country": "US", // confirmationCost.FK_ConCountry
//           "state": "PA", // confirmationCost.FK_ConState
//           "zip": "19079" // confirmationCost.ConZip
//         },
//         "companyName": "Freight Force PHL", // confirmationCost.ConName
//         "scheduledDate": 1638176400000, // check from code
//         "specialInstructions": "" // check from code
//       }
//     ],
//     "dockHigh": "N", //  [Y / N] default "N"
//     "hazardous": "N", //   shipmentDesc?.Hazmat
//     "liftGate": "N", //  shipmentApar.ChargeCode
//     "unNum": "" // shipmentDesc.Description accepts only 4 degit number as string or empty string
//   }
// }
