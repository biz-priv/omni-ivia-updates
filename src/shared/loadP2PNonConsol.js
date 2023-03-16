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
  CONSIGNEE_TABLE,
  SHIPPER_TABLE,
  INSTRUCTIONS_TABLE,
  INSTRUCTIONS_INDEX_KEY_NAME,
  IVIA_DDB,
  IVIA_VENDOR_ID,
  // IVIA_CARRIER_ID,
} = process.env;
const IVIA_CARRIER_ID = "102"; //NOTE:- for stage IVIA need to change it later

const loadP2PNonConsol = async (dynamoData, shipmentAparData) => {
  console.log("load-P2P-Non-Consol");

  //get the primary key and All table list
  const { tableList, primaryKeyValue } = getTablesAndPrimaryKey(
    dynamoData.dynamoTableName,
    dynamoData
  );

  /**
   * get data from all the requied tables
   * shipmentApar
   * confirmationCost
   * shipmentHeader
   * shipmentDesc
   */
  const dataSet = await fetchDataFromTables(tableList, primaryKeyValue);
  console.log("dataSet", JSON.stringify(dataSet));

  const shipmentApar = shipmentAparData;
  const shipmentHeader = dataSet.shipmentHeader;
  const consignee = dataSet.consignee.length > 0 ? dataSet.consignee[0] : {};
  const shipper = dataSet.shipper.length > 0 ? dataSet.shipper[0] : {};
  const shipmentInstructions = dataSet.shipmentInstructions;

  //only used for liftgate
  const shipmentAparCargo = dataSet.shipmentAparCargo;

  // pick data from shipment_desc based on consol no = 0
  const confirmationCost = dataSet.confirmationCost.filter(
    (e) => e.ConsolNo === "0"
  );

  // pick data from shipment_desc based on consol no = 0
  const shipmentDesc = dataSet.shipmentDesc.filter((e) => e.ConsolNo === "0");

  /**
   * if con cost don't have any data then pick data from shipper and consignee
   */
  let ptype, dtype;
  if (confirmationCost.length > 0) {
    //filtering confirmationCost based on FK_OrderNo and SeqNo and Consolidation from table shipmentApar
    const data = confirmationCost.filter((e) => {
      return (
        e.FK_OrderNo === shipmentApar.FK_OrderNo &&
        e.FK_SeqNo === shipmentApar.SeqNo &&
        shipmentApar.Consolidation === "N"
      );
    });
    ptype = JSON.parse(JSON.stringify(data));
    dtype = JSON.parse(JSON.stringify(data));
  } else {
    ptype = {
      ...shipper,
      PickupTimeRange: shipmentHeader?.ReadyDateTimeRange ?? "",
      PickupDateTime: shipmentHeader?.ReadyDateTime ?? "",
      PickupNote: shipmentInstructions
        .filter(
          (si) =>
            si.Type.toUpperCase() === "P" &&
            si.FK_OrderNo === shipmentApar.FK_OrderNo
        )
        .map((ei) => ei.Note)
        .join(" "),
    };
    dtype = {
      ...consignee,
      DeliveryTimeRange: shipmentHeader?.ScheduledDateTimeRange ?? "",
      DeliveryDateTime: shipmentHeader?.ScheduledDateTime ?? "",
      DeliveryNote: shipmentInstructions
        .filter(
          (si) =>
            si.Type.toUpperCase() === "D" &&
            si.FK_OrderNo === shipmentApar.FK_OrderNo
        )
        .map((ei) => ei.Note)
        .join(" "),
    };
  }

  // NOTE:- check this one when we implement full error notification
  // exactly one shipfrom/ to address in tbl_confirmation_cost for file number
  // if (filteredConfirmationCost.length > 1) {
  //   console.log("error: multiple line on confirmationCost");
  //   return {};
  // }

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

  const pStopTypeData = {
    stopType: "P",
    stopNum: 0,
    housebills: housebill_delimited,
    address: {
      address1: ptype.ShipAddress1,
      address2: ptype.ShipAddress2,
      city: ptype.ShipCity,
      country: ptype.FK_ShipCountry,
      state: ptype.FK_ShipState,
      zip: ptype.ShipZip,
    },
    companyName: ptype.ShipName,
    cargo: cargo,
    scheduledDate: await getGMTDiff(
      ptype.PickupDateTime,
      ptype.ShipZip,
      ptype.FK_ShipCountry
    ),
    specialInstructions: (
      getNotesP2Pconsols(ptype.PickupTimeRange, ptype.PickupDateTime, "p") +
      "\r\n" +
      ptype.PickupNote
    ).slice(0, 200),
  };

  const dStopTypeData = {
    stopType: "D",
    stopNum: 1,
    housebills: housebill_delimited,
    address: {
      address1: dtype.ConAddress1,
      address2: dtype.ConAddress2,
      city: dtype.ConCity,
      country: dtype.FK_ConCountry,
      state: dtype.FK_ConState,
      zip: dtype.ConZip,
    },
    companyName: dtype.ConName,
    scheduledDate: await getGMTDiff(
      dtype.DeliveryDateTime,
      dtype.ConZip,
      dtype.FK_ConCountry
    ),
    specialInstructions: (
      getNotesP2Pconsols(dtype.DeliveryTimeRange, dtype.DeliveryDateTime, "d") +
      "\r\n" +
      dtype.DeliveryNote
    ).slice(0, 200),
  };

  /**
   * preparing pickup type and delivery type stopes obj from table ConfirmationCost
   */
  // const fcc = JSON.parse(JSON.stringify(filteredConfirmationCost));
  // let pStopTypeData = [],
  //   dStopTypeData = [];
  // for (let index = 0; index < fcc.length; index++) {
  //   const e = fcc[index];

  //   pickup type stopes
  //   pStopTypeData = [
  //     ...pStopTypeData,
  //     {
  //       stopType: "P",
  //       stopNum: 0,
  //       housebills: housebill_delimited,
  //       address: {
  //         address1: e.ShipAddress1,
  //         address2: e.ShipAddress2,
  //         city: e.ShipCity,
  //         country: e.FK_ShipCountry,
  //         state: e.FK_ShipState,
  //         zip: e.ShipZip,
  //       },
  //       companyName: e.ShipName,
  //       cargo: cargo,
  //       scheduledDate: await getGMTDiff(
  //         e.PickupDateTime,
  //         e.ShipZip,
  //         e.FK_ShipCountry
  //       ),
  //       specialInstructions: (
  //         getNotesP2Pconsols(e.PickupTimeRange, e.PickupDateTime, "p") +
  //         "\r\n" +
  //         e.PickupNote
  //       ).slice(0, 200),
  //     },
  //   ];

  //   delivery type stopes
  //   dStopTypeData = [
  //     ...dStopTypeData,
  //     {
  //       stopType: "D",
  //       stopNum: 1,
  //       housebills: housebill_delimited,
  //       address: {
  //         address1: e.ConAddress1,
  //         address2: e.ConAddress2,
  //         city: e.ConCity,
  //         country: e.FK_ConCountry,
  //         state: e.FK_ConState,
  //         zip: e.ConZip,
  //       },
  //       companyName: e.ConName,
  //       scheduledDate: await getGMTDiff(
  //         e.DeliveryDateTime,
  //         e.ConZip,
  //         e.FK_ConCountry
  //       ),
  //       specialInstructions: (
  //         getNotesP2Pconsols(e.DeliveryTimeRange, e.DeliveryDateTime, "d") +
  //         "\r\n" +
  //         e.DeliveryNote
  //       ).slice(0, 200),
  //     },
  //   ];
  // }

  /**
   * filtered shipmentDesc data based on shipmentApar.FK_OrderNo to get hazardous and unNum
   */
  const ORDER_NO_LIST = shipmentApar.FK_OrderNo;
  const filteredSD = shipmentDesc.filter((e) =>
    ORDER_NO_LIST.includes(e.FK_OrderNo)
  );

  //IVIA payload
  const iviaPayload = {
    carrierId: IVIA_CARRIER_ID, // IVIA_CARRIER_ID = dev 1000025 stage = 102
    refNums: {
      refNum1: housebill_delimited[0] ?? "", // tbl_shipmentHeader.pk_orderNo as hwb/ tbl_confirmationCost.consolNo(if it is a consol)
      refNum2: "", //ignore
    },
    shipmentDetails: {
      stops: [...pStopTypeData, ...dStopTypeData],
      dockHigh: "N", // req [Y / N]
      hazardous: getHazardous(filteredSD),
      liftGate: getLiftGate(shipmentAparCargo),
      unNum: getUnNum(filteredSD), // accepts only 4 degit number as string
    },
  };
  console.log("iviaPayload", JSON.stringify(iviaPayload));

  /**
   * validate the payload and check if it is already processed
   */
  const { check, errorMsg, isError } = await validateAndCheckIfDataSentToIvia(
    iviaPayload,
    shipmentApar
  );
  if (!check) {
    if (isError) {
      await sendSNSMessage(iviaTableData);
    }
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
      ConsolNo: shipmentAparData?.ConsolNo,
      FK_OrderNo: shipmentAparData?.FK_OrderNo,
      payloadType: "P2PNonConsol",
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
  } else {
    console.log("Already sent to IVIA or validation error");
  }
};

/**
 * validate the payload structure and check from dynamodb if the data is sent to ivia priviously.
 * @param {*} payload
 * @param {*} shipmentApar
 * @returns 3 variables
 *  1> check :- true/false  if omni-ivia don't have record with success status then false else true
 *  2> isError: true/false if we have validation then true else false
 *  3> errorMsg: "" if isErroris true then this variable will contain the validation error msg
 */
function validateAndCheckIfDataSentToIvia(payload, shipmentApar) {
  return new Promise(async (resolve, reject) => {
    //validate and get the errorMsg if any validation error happens
    let errorMsg = validatePayload(payload);
    console.log("errorMsg", errorMsg);

    try {
      //fetch from ivia table and check if data processed or not
      const params = {
        TableName: IVIA_DDB,
        IndexName: "omni-ivia-ConsolNo-index",
        KeyConditionExpression: "ConsolNo = :ConsolNo",
        FilterExpression: "FK_OrderNo = :FK_OrderNo",
        ExpressionAttributeValues: {
          ":ConsolNo": shipmentApar.ConsolNo.toString(),
          ":FK_OrderNo": shipmentApar.FK_OrderNo.toString(),
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
      [CONSIGNEE_TABLE]: {
        PK: "FK_ConOrderNo",
        SK: "",
        sortName: "consignee",
        type: "INDEX",
      },
      [SHIPPER_TABLE]: {
        PK: "FK_ShipOrderNo",
        SK: "",
        sortName: "shipper",
        type: "INDEX",
      },
    };

    const data = tableList[tableName];
    const primaryKeyValue =
      data.type === "INDEX"
        ? dynamoData.NewImage[data.indexKeyColumnName].S
        : dynamoData.Keys[data.PK].S;

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

    /**
     * shipmentInstructions
     */
    let shipmentInstructions = [];
    const FK_OrderNoListForIns = [
      ...new Set(newObj.shipmentApar.map((e) => e.FK_OrderNo)),
    ];
    for (let index = 0; index < FK_OrderNoListForIns.length; index++) {
      const element = FK_OrderNoListForIns[index];

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
    }
    newObj.shipmentInstructions = shipmentInstructions;
    /**
     * Fetch shipment apar for liftgate based on shipmentDesc.FK_OrderNo
     */
    const FK_OrderNoList = [
      ...new Set(newObj.shipmentDesc.map((e) => e.FK_OrderNo)),
    ];
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
    console.log("shipmentAparCargo", shipmentAparCargo);
    newObj.shipmentAparCargo = shipmentAparCargo;
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
