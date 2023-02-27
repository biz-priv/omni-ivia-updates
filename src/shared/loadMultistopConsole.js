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
  STAGE,
} = process.env;

const globalConsolIndex = "omni-ivia-ConsolNo-index-" + STAGE;
/**
 * multistop console //non consol p2p // send console no
 */
const loadMultistopConsole = async (dynamoData, shipmentAparData) => {
  console.log("loadMultistopConsole");

  const CONSOL_NO = shipmentAparData.ConsolNo;

  const dd = await fetchDataFromTablesList(CONSOL_NO);
  console.log("dd", JSON.stringify(dd));

  const dataSet = await fetchDataFromTablesList(CONSOL_NO);
  console.log("dataSet", JSON.stringify(dataSet));

  const shipmentApar = dataSet.shipmentApar;
  const shipmentHeader = dataSet.shipmentHeader;
  const shipmentDesc = dataSet.shipmentDesc;
  const consolStopHeaders = dataSet.consolStopHeaders;
  const consolStopItems = dataSet.consolStopItems;
  const shipmentInstructions = dataSet.shipmentInstructions;

  const ORDER_NO_LIST = shipmentApar.map((e) => e.FK_OrderNo);

  let dataArr = [];
  consolStopItems.map((e) => {
    const csh = consolStopHeaders
      .filter((es) => es.PK_ConsolStopId === e.FK_ConsolStopId)
      .map((es) => {
        let houseBillList = shipmentHeader.filter(
          (esh) => esh.PK_OrderNo === e.FK_OrderNo
        );
        houseBillList =
          houseBillList.length > 0 ? houseBillList[0].Housebill : "";
        return { ...es, ...e, Housebill: houseBillList };
      });
    dataArr = [...dataArr, ...csh];
  });
  // console.log("dataArr", dataArr);

  const pTypeShipment = groupBy(
    dataArr.filter((e) => e.ConsolStopPickupOrDelivery === "false"),
    "ConsolStopNumber"
  );

  const dTypeShipment = groupBy(
    dataArr.filter((e) => e.ConsolStopPickupOrDelivery === "true"),
    "ConsolStopNumber"
  );

  const pTypeShipmentMap = Object.keys(pTypeShipment).map((e) => {
    const ele = pTypeShipment[e];
    const csh = ele[0];
    const cargo = getCargoData(shipmentDesc, ele);
    // Notes
    const sInsNotes = shipmentInstructions
      .filter(
        (si) =>
          si.Type.toUpperCase() === "P" && si.FK_OrderNo === csh.FK_OrderNo
      )
      .map((ei) => ei.Note)
      .join(" ");

    const stopPayload = {
      stopType: "P",
      stopNum: e,
      housebills: [...new Set(ele.map((e) => e.Housebill))],
      address: {
        address1: csh.ConsolStopAddress1,
        city: csh.ConsolStopCity,
        country: csh.FK_ConsolStopCountry,
        state: csh.FK_ConsolStopState,
        zip: csh.ConsolStopZip,
      },
      companyName: csh?.ConsolStopName,
      cargo: cargo,
      scheduledDate: getGMTDiff(
        csh.ConsolStopDate.split(" ")[0] +
          " " +
          (csh.ConsolStopTimeBegin.split(" ")?.[1] ?? "")
      ),
      specialInstructions:
        (csh.ConsolStopAddress2 === "" ? "" : csh.ConsolStopAddress2 + " ") +
        sInsNotes,
    };
    return stopPayload;
  });

  const dTypeShipmentMap = Object.keys(dTypeShipment).map((e) => {
    const ele = dTypeShipment[e];
    const csh = ele[0];
    const sInsNotes = shipmentInstructions
      .filter(
        (si) =>
          si.Type.toUpperCase() === "D" && si.FK_OrderNo === csh.FK_OrderNo
      )
      .map((ei) => ei.Note)
      .join(" ");

    const stopPayload = {
      stopType: "D",
      stopNum: e,
      housebills: [...new Set(ele.map((e) => e.Housebill))],
      address: {
        address1: csh.ConsolStopAddress1,
        city: csh.ConsolStopCity,
        country: csh.FK_ConsolStopCountry,
        state: csh.FK_ConsolStopState,
        zip: csh.ConsolStopZip,
      },
      companyName: csh?.ConsolStopName,
      scheduledDate: getGMTDiff(
        csh.ConsolStopDate.split(" ")?.[0] +
          " " +
          (csh.ConsolStopTimeBegin.split(" ")?.[1] ?? "")
      ),
      specialInstructions:
        (csh.ConsolStopAddress2 === "" ? "" : csh.ConsolStopAddress2 + " ") +
        sInsNotes,
    };
    return stopPayload;
  });

  const filteredSH = shipmentDesc.filter((e) =>
    ORDER_NO_LIST.includes(e.FK_OrderNo)
  );

  const iviaPayload = {
    carrierId: IVIA_CARRIER_ID, // IVIA_CARRIER_ID = 1000025
    refNums: {
      refNum1: CONSOL_NO ?? "", // tbl_shipmentHeader.pk_orderNo as hwb/ tbl_confirmationCost.consolNo(if it is a consol)
      refNum2: "", // as query filenumber value is always "" hardcode
    },
    shipmentDetails: {
      stops: [...pTypeShipmentMap, ...dTypeShipmentMap],
      dockHigh: "N", // req [Y / N]
      hazardous: getHazardous(filteredSH),
      liftGate: getLiftGate(shipmentApar),
      unNum: getUnNum(filteredSH), // accepts only 4 degit number as string
    },
  };
  console.log("iviaPayload", JSON.stringify(iviaPayload));
  const check = await validateAndCheckIfDataSentToIvia(iviaPayload, CONSOL_NO);
  console.log("check", check);
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
      FK_OrderNo: ORDER_NO_LIST.join(","),
      payloadType: "loadMultistopConsole",
      InsertedTimeStamp: momentTZ
        .tz("America/Chicago")
        .format("YYYY:MM:DD HH:mm:ss")
        .toString(),
    };
    console.log("iviaTableData", iviaTableData);
    await putItem(IVIA_DDB, iviaTableData);
  }
};

function groupBy(xs, key) {
  return xs.reduce(function (rv, x) {
    (rv[x[key]] = rv[x[key]] || []).push(x);
    return rv;
  }, {});
}

/**
 * cargo
 */
function getCargoData(shipmentDesc, ele) {
  const fkOrderNoList = ele.map((e) => e.FK_OrderNo);
  return shipmentDesc
    .filter((e) => fkOrderNoList.includes(e.FK_OrderNo))
    .map((e) => ({
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
    }));
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
          ":status": getStatus().SUCCESS,
        },
      };
      console.log("params", params);
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
        "FK_VendorId = :FK_VendorId and Consolidation = :Consolidation and FK_ServiceId = :FK_ServiceId",
      ExpressionAttributeValues: {
        ":ConsolNo": CONSOL_NO.toString(),
        ":FK_VendorId": IVIA_VENDOR_ID.toString(),
        ":Consolidation": "N",
        ":FK_ServiceId": "MT",
      },
    };

    let shipmentApar = await ddb.query(sapparams).promise();
    shipmentApar = shipmentApar.Items;

    let shipmentInstructions = [],
      shipmentHeader = [],
      shipmentDesc = [],
      consolStopHeaders = [],
      consolStopItems = [];
    for (let index = 0; index < shipmentApar.length; index++) {
      const element = shipmentApar[index];
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
       * shipmentHeader
       */
      const shparams = {
        TableName: SHIPMENT_HEADER_TABLE,
        KeyConditionExpression: "PK_OrderNo = :PK_OrderNo",
        ExpressionAttributeValues: {
          ":PK_OrderNo": element.FK_OrderNo.toString(),
        },
      };
      let sh = await ddb.query(shparams).promise();
      shipmentHeader = [...shipmentHeader, ...sh.Items];

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

    return {
      shipmentApar,
      shipmentInstructions,
      shipmentHeader,
      shipmentDesc,
      consolStopHeaders,
      consolStopItems,
    };
  } catch (error) {
    console.log("error", error);
    return {};
  }
}

module.exports = { loadMultistopConsole };
