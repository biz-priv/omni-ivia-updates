/*
* File: src\shared\loadP2PNonConsol.js
* Project: Omni-ivia-updates
* Author: Bizcloud Experts
* Date: 2024-03-01
* Confidential and Proprietary
*/
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
const moment = require("moment");
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
  CONFIRMATION_COST,
  CONFIRMATION_COST_INDEX_KEY_NAME,
  CONSIGNEE_TABLE,
  SHIPPER_TABLE,
  CUSTOMER_TABLE,
  INSTRUCTIONS_TABLE,
  INSTRUCTIONS_INDEX_KEY_NAME,
  IVIA_DDB,
  EQUIPMENT_TABLE,
  IVIA_VENDOR_ID,
  IVIA_CARRIER_ID,
} = process.env;
// const IVIA_CARRIER_ID = "102"; //NOTE:- for stage IVIA need to change it later

const loadP2PNonConsol = async (dynamoData, shipmentAparData) => {
  console.info("load-P2P-Non-Consol");

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

  const shipmentApar = shipmentAparData;
  const shipmentHeader = dataSet.shipmentHeader;
  const consignee = dataSet.consignee.length > 0 ? dataSet.consignee[0] : {};
  const shipper = dataSet.shipper.length > 0 ? dataSet.shipper[0] : {};
  const shipmentInstructions = dataSet.shipmentInstructions;
  const equipment = dataSet.equipment.length > 0 ? dataSet.equipment[0] : {};
  const customer = get(dataSet, "customer[0]", {})

  /**
   * we need to check in the shipmentHeader.OrderDate >= '2023:04:01 00:00:00' - for both nonconsol and consol -> if this condition satisfies, we send the event to Ivia, else we ignore
   * Ignore the event if there is no OrderDate or it is "1900
   */
  if (!checkIfShipmentHeaderOrderDatePass(shipmentHeader)) {
    console.info(
      "event IGNORED shipmentHeader.OrderDate LESS THAN 2023:04:01 00:00:00 "
    );
    return {};
  }

  //only used for liftgate
  const shipmentAparCargo = dataSet.shipmentAparCargo;

  // pick data from shipment_desc based on consol no = 0
  const shipmentDesc = dataSet.shipmentDesc.filter((e) => e.ConsolNo === "0");

  //fetch notes from Instructions table based on shipment_apar table  FK_OrderNo data
  // getting pickup type notes based on Type == "P"
  const pInsNotes = shipmentInstructions
    .filter(
      (si) =>
        si.Type.toUpperCase() === "P" &&
        si.FK_OrderNo === shipmentApar.FK_OrderNo
    )
    .map((ei) => ei.Note)
    .join(" ");

  // getting delivery type notes based on Type == "D"
  const dInsNotes = shipmentInstructions
    .filter(
      (si) =>
        si.Type.toUpperCase() === "D" &&
        si.FK_OrderNo === shipmentApar.FK_OrderNo
    )
    .map((ei) => ei.Note)
    .join(" ");

  /**
   * if confirmationCost don't have any data then pick data from shipper and consignee table
   */
  let ptype, dtype;
  ptype = {
    ...shipper,
    PickupTimeRange: shipmentHeader?.[0]?.ReadyDateTimeRange ?? "",
    PickupDateTime: shipmentHeader?.[0]?.ReadyDateTime ?? "",
    ScheduledBy: shipmentHeader?.[0]?.ScheduledBy ?? "",
    PickupNote: pInsNotes,
  };
  dtype = {
    ...consignee,
    DeliveryTimeRange: shipmentHeader?.[0]?.ScheduledDateTimeRange ?? "",
    DeliveryDateTime: shipmentHeader?.[0]?.ScheduledDateTime ?? "",
    ScheduledBy: shipmentHeader?.[0]?.ScheduledBy ?? "",
    DeliveryNote: dInsNotes,
  };
  // }

  console.info("ptype", ptype);
  console.info("dtype", dtype);

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
        quantity: e?.Pieces ?? 0,
        length: checkIfZero ? 1 : parseInt(e?.Length),
        width: checkIfZero ? 1 : parseInt(e?.Width),
        height: checkIfZero ? 1 : parseInt(e?.Height),
        weight: e?.Weight ? parseInt(e?.Weight) : 0,
        stackable: "Y", // hardcode
        turnable: "Y", // hardcode
      };
    })
    .filter((e) => e.quantity != "" && e.quantity != 0 && e.quantity != "0");

  /**
   * preparing pickup type stop obj from table ConfirmationCost based on shipmentAPAR.FK_OrderNo
   * if ConfirmationCost table don't have data then pick data from tbl_shipper and tbl_shipmentHeader table based on shipmentAPAR.FK_OrderNo
   * and PickupTimeRange value mapped with =  tbl_shipmentHeader.ReadyDateTimeRange
   * and PickupDateTime value mapped with = tbl_shipmentHeader.ReadyDateTime
   * and PickupNote:- only tbl_shipmentInstructions.Note , Type = "P"
   * scheduledDate: check getGMTDiff() function
   * specialInstructions:- check getNotesP2Pconsols() function and added PickupNote
   */
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
    scheduledDate: "",
    specialInstructions: (
      getNotesP2Pconsols(ptype.PickupTimeRange, ptype.PickupDateTime, "p") +
      "\r\n" +
      (ptype.ShipContact.length > 0 || ptype.ShipPhone.length > 0
        ? "Contact " + ptype.ShipContact + " " + ptype.ShipPhone + "\r\n"
        : "") +
      ptype.PickupNote
    ).slice(0, 200),
    cutoffDate: "",
  };

  const ptypeAddressData = await checkAddressByGoogleApi(pStopTypeData.address);
  pStopTypeData.address = ptypeAddressData;
  pStopTypeData.scheduledDate = await getGMTDiff(
    ptype.PickupDateTime,
    ptypeAddressData
  );

  if (ptype.PickupTimeRange) {
    if (ptype.PickupTimeRange.slice(11) == "00:00:00.000") {
      pStopTypeData.cutoffDate = null
    } else {
      pStopTypeData.cutoffDate = await getGMTDiff(
        ptype.PickupTimeRange,
        ptypeAddressData
      );
    }
  } else {
    pStopTypeData.cutoffDate = null
  }

  /**
   * preparing delivery type stop obj from table ConfirmationCost based on shipmentAPAR.FK_OrderNo
   * if ConfirmationCost table don't have data then pick data from tbl_consignee and tbl_shipmentHeader table based on tbl_shipmentAPAR.FK_OrderNo
   * and DeliveryTimeRange value mapped with =  tbl_shipmentHeader.ScheduledDateTimeRange
   * and DeliveryDateTime value mapped with = tbl_shipmentHeader.ScheduledDateTime
   * and DeliveryNote:- only tbl_shipmentInstructions.Note,  Type = "D"
   * scheduledDate: check getGMTDiff() function
   * specialInstructions:- check getNotesP2Pconsols() function and added DeliveryNote
   */
  let delNotes = "";
  if (dtype.ScheduledBy == "T") {
    delNotes =
      "Deliver between " +
      moment(dtype.DeliveryDateTime).format("HH:mm") +
      " and " +
      moment(dtype.DeliveryTimeRange).format("HH:mm");
  } else if (dtype.ScheduledBy == "B") {
    delNotes = "Deliver by " + moment(dtype.DeliveryDateTime).format("HH:mm");
  } else {
    delNotes = "Deliver at " + moment(dtype.DeliveryDateTime).format("HH:mm");
  }
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
    scheduledDate: "",
    specialInstructions: (
      delNotes +
      "\r\n" +
      (dtype.ConContact.length > 0 || dtype.ConPhone.length > 0
        ? "Contact " + dtype.ConContact + " " + dtype.ConPhone + "\r\n"
        : "") +
      dtype.DeliveryNote
    ).slice(0, 200),
    cutoffDate: "",
  };

  const dtypeAddressData = await checkAddressByGoogleApi(dStopTypeData.address);
  dStopTypeData.address = dtypeAddressData;
  dStopTypeData.scheduledDate = await getGMTDiff(
    dtype.DeliveryDateTime,
    dtypeAddressData
  );

  if (dtype.DeliveryTimeRange) {
    if (dtype.DeliveryTimeRange.slice(11) == "00:00:00.000") {
      dStopTypeData.cutoffDate = null
    } else {
      dStopTypeData.cutoffDate = await getGMTDiff(
        dtype.DeliveryTimeRange,
        dtypeAddressData
      );
    }
  } else {
    dStopTypeData.cutoffDate = null
  }



  /**
   * filtered shipmentDesc data based on shipmentApar.FK_OrderNo to get hazardous and unNum
   */
  const ORDER_NO_LIST = shipmentApar.FK_OrderNo;
  const filteredSD = shipmentDesc.filter((e) =>
    ORDER_NO_LIST.includes(e.FK_OrderNo)
  );
  // const total = dataSet.shipmentApar[0].Total

  let totalArray = []
  for (let i = 0; i < dataSet.shipmentApar.length; i++) {
    if(dataSet.shipmentApar[i].FK_VendorId==IVIA_VENDOR_ID){
      const element = dataSet.shipmentApar[i].Total;
      totalArray.push(element)
    }
  }

  const total = totalArray.reduce((accumulator, currentValue) => {
    return accumulator + parseFloat(currentValue);
  }, 0);


  const instructionNoteArray = shipmentInstructions.filter(
    (esh) => esh.Type === "S"
  );
  const instructionNote = "\r\n" + get(instructionNoteArray[0], "Note", "")

  const notesValue = get(equipment,"Description", "") + "\r\n" + get(instructionNoteArray[0], "Note", "")
  console.info("notes", notesValue)


  //IVIA payload
  const iviaPayload = {
    carrierId: IVIA_CARRIER_ID, // IVIA_CARRIER_ID = dev 1000025 stage = 102
    refNums: {
      refNum1: housebill_delimited[0] ?? "", // tbl_shipmentHeader.pk_orderNo as hwb/ tbl_confirmationCost.consolNo(if it is a consol)
      refNum2: customer?.CustName?.slice(0, 19) ?? "", //customee name
      refNum3: get(shipmentHeader[0], "HandlingStation", ""), //HandlingStation
    },
    shipmentDetails: {
      stops: [pStopTypeData, dStopTypeData],
      dockHigh: "N", // req [Y / N]
      hazardous: getHazardous(filteredSD),
      liftGate: getLiftGate(shipmentAparCargo, shipmentHeader),
      unNum: getUnNum(filteredSD), // accepts only 4 degit number as string
      notes: notesValue.slice(0,199),
      revenue: +parseFloat(total).toFixed(2) ?? "",
    },
  };
  console.info("iviaPayload", JSON.stringify(iviaPayload));

  /**
   * validate the payload and check if it is already processed
   */
  const { check, errorMsg, isError } = await validateAndCheckIfDataSentToIvia(
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
    if (isError) {
      /**
       *
       */
      await sendSNSMessage(iviaTableData);
    }
    await putItem(IVIA_DDB, iviaTableData);
  } else {
    console.info("Already sent to IVIA or validation error");
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
    console.info("errorMsg", errorMsg);

    try {
      //fetch from ivia table and check if data processed or not
      const params = {
        TableName: IVIA_DDB,
        IndexName: "omni-ivia-ConsolNo-FK_OrderNo-index",
        KeyConditionExpression:
          "ConsolNo = :ConsolNo and FK_OrderNo = :FK_OrderNo",
        ExpressionAttributeValues: {
          ":ConsolNo": shipmentApar.ConsolNo.toString(),
          ":FK_OrderNo": shipmentApar.FK_OrderNo.toString(),
        },
      };
      const data = await ddb.query(params).promise();
      console.info("data:ivia", data);

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
      console.error("dynamoError:", error);
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
        type: "PRIMARY_KEY",
      },
      [SHIPPER_TABLE]: {
        PK: "FK_ShipOrderNo",
        SK: "",
        sortName: "shipper",
        type: "PRIMARY_KEY",
      },
      [CUSTOMER_TABLE]: {
        PK: "PK_CustNo",
        SK: "",
        sortName: "customer",
        type: "PRIMARY_KEY",
      },
    };

    const data = tableList[tableName];
    const primaryKeyValue =
      data.type === "INDEX"
        ? dynamoData.NewImage[data.indexKeyColumnName].S
        : dynamoData.Keys[data.PK].S;

    return { tableList, primaryKeyValue };
  } catch (error) {
    console.error("error:unable to select table",tableName, error);
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
          ":FK_OrderNo": element.toString(),
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
    newObj.shipmentAparCargo = shipmentAparCargo;

    /**
     * EQUIPMENT_TABLE
     */
    let equipment = [];
    if (
      newObj.shipmentHeader.length > 0 &&
      newObj.shipmentHeader[0].FK_EquipmentCode != ""
    ) {
      const equipmentParam = {
        TableName: EQUIPMENT_TABLE,
        KeyConditionExpression: "PK_EquipmentCode = :PK_EquipmentCode",
        ExpressionAttributeValues: {
          ":PK_EquipmentCode":
            newObj.shipmentHeader[0].FK_EquipmentCode.toString(),
        },
      };

      equipment = await ddb.query(equipmentParam).promise();
      equipment = equipment.Items;
    }
    newObj.equipment = equipment;

    /**
     * CUSTOMER_TABLE
     */
    let customer = []
    if (
      newObj.shipmentHeader.length > 0 &&
      newObj.shipmentHeader[0].BillNo != ""
    ) {
      const customerParam = {
        TableName: CUSTOMER_TABLE,
        KeyConditionExpression: "PK_CustNo = :PK_CustNo",
        ExpressionAttributeValues: {
          ":PK_CustNo": newObj.shipmentHeader[0].BillNo,
        },
      };

      customer = await ddb.query(customerParam).promise();
      customer = customer.Items;
    }
    newObj.customer = customer;

    return newObj;
  } catch (error) {
    console.error("error:fetchDataFromTables", error);
  }
}

module.exports = { loadP2PNonConsol };
