const AWS = require("aws-sdk");
const {
  getLiftGate,
  getUnNum,
  validatePayload,
  getHazardous,
} = require("./dataHelper");
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
  STAGE,
} = process.env;

const globalConsolIndex = "omni-ivia-ConsolNo-index-" + STAGE;
/**
 * multistop console //non consol p2p // send console no
 */
const loadMultistopConsole = async (dynamoData, shipmentAparData) => {
  console.log("loadMultistopConsole");

  const CONSOL_NO = shipmentAparData.ConsolNo;

  const tableList = getTablesAndPrimaryKey(dynamoData);
  const dataSet = await fetchDataFromTables(tableList, CONSOL_NO);
  console.log("dataSet", JSON.stringify(dataSet));

  const shipmentApar = dataSet.shipmentApar.filter(
    (e) => e.Consolidation === "N"
  );

  const shipmentHeader = dataSet.shipmentHeader;
  const shipmentDesc = dataSet.shipmentDesc;
  const ORDER_NO_LIST = shipmentApar.map((e) => e.FK_OrderNo);

  const consolStopItems = dataSet.consolStopItems.filter((e) =>
    ORDER_NO_LIST.includes(e.FK_OrderNo)
  );
  const consolStopHeaders = dataSet.consolStopHeaders.filter((e) =>
    consolStopItems.map((cs) => cs.FK_ConsolStopId).includes(e.PK_ConsolStopId)
  );
  const shipmentInstructions = dataSet.shipmentInstructions.filter((e) =>
    ORDER_NO_LIST.includes(e.FK_OrderNo)
  );
  let dataArr = [];
  consolStopItems.map((e) => {
    const csh = consolStopHeaders
      .filter((es) => es.PK_ConsolStopId === e.FK_ConsolStopId)
      .map((es) => {
        let houseBillList = shipmentHeader.filter(
          (esh) => esh.PK_OrderNo === e.FK_OrderNo
        );
        console.log("houseBillList", houseBillList);
        houseBillList =
          houseBillList.length > 0 ? houseBillList[0].Housebill : "";
        return { ...es, ...e, Housebill: houseBillList };
      });
    console.log("csh", csh);
    dataArr = [...dataArr, ...csh];
  });
  console.log("dataArr", dataArr);

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
      .filter((si) => si.Type === "P" && si.FK_OrderNo === csh.FK_OrderNo)
      .map((ei) => ei.Note)
      .join(" ");

    const stopPayload = {
      stopType: "P",
      stopNum: e,
      housebills: ele.map((e) => e.Housebill),
      address: {
        address1: csh.ConsolStopAddress1,
        city: csh.ConsolStopCity,
        country: csh.FK_ConsolStopCountry,
        state: csh.FK_ConsolStopState,
        zip: csh.ConsolStopZip,
      },
      companyName: csh?.ConsolStopName,
      cargo: cargo,
      scheduledDate: moment(
        csh.ConsolStopDate.split(" ")[0] +
          " " +
          csh.ConsolStopTimeBegin.split(" ")[1]
      ).diff("1970-01-01", "ms"),
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
      .filter((si) => si.Type === "D" && si.FK_OrderNo === csh.FK_OrderNo)
      .map((ei) => ei.Note)
      .join(" ");

    const stopPayload = {
      stopType: "D",
      stopNum: e,
      housebills: ele.map((e) => e.Housebill),
      address: {
        address1: csh.ConsolStopAddress1,
        city: csh.ConsolStopCity,
        country: csh.FK_ConsolStopCountry,
        state: csh.FK_ConsolStopState,
        zip: csh.ConsolStopZip,
      },
      companyName: csh?.ConsolStopName,
      scheduledDate: moment(
        csh.ConsolStopDate.split(" ")[0] +
          " " +
          csh.ConsolStopTimeBegin.split(" ")[1]
      ).diff("1970-01-01", "ms"),
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
        indexKeyName: globalConsolIndex,
        type: "INDEX",
      },
      [INSTRUCTIONS_TABLE]: {
        PK: "PK_InstructionNo",
        SK: "",
        sortName: "shipmentInstructions",
        indexKeyColumnName: "ConsolNo",
        indexKeyName: globalConsolIndex,
        type: "INDEX",
      },
      [SHIPMENT_DESC_TABLE]: {
        PK: "FK_OrderNo",
        SK: "SeqNo",
        sortName: "shipmentDesc",
        indexKeyColumnName: "ConsolNo",
        indexKeyName: globalConsolIndex,
        type: "INDEX",
      },
      [CONFIRMATION_COST]: {
        PK: "PK_ConfirmationNo",
        SK: "FK_OrderNo",
        sortName: "confirmationCost",
        indexKeyColumnName: "ConsolNo",
        indexKeyName: globalConsolIndex,
        type: "INDEX",
      },
      [CONSOL_STOP_HEADERS]: {
        PK: "FK_OrderNo",
        SK: "FK_ConsolStopId",
        sortName: "consolStopHeaders",
        indexKeyColumnName: "FK_ConsolNo",
        indexKeyName: "omni-ivia-FK_ConsolNo-index" + STAGE,
        type: "INDEX",
      },
      // [CONSOL_STOP_ITEMS]: {
      //   PK: "FK_OrderNo",
      //   SK: "FK_ConsolStopId",
      //   sortName: "consolStopItems",
      //   indexKeyColumnName: "ConsolNo",
      //   indexKeyName: globalConsolIndex,
      //   type: "INDEX",
      // },
      // [SHIPMENT_HEADER_TABLE]: {
      //   PK: "PK_OrderNo",
      //   SK: "",
      //   sortName: "shipmentHeader",
      //   indexKeyColumnName: "ConsolNo",
      //   indexKeyName: globalConsolIndex,
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

module.exports = { loadMultistopConsole };
