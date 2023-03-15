const AWS = require("aws-sdk");
const {
  getLiftGate,
  getUnNum,
  validatePayload,
  getHazardous,
  getGMTDiff,
  getStatus,
  sortObjByStopNo,
} = require("./dataHelper");
const moment = require("moment");
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
  INSTRUCTIONS_TABLE,
  SHIPMENT_DESC_TABLE,
  CONSOL_STOP_HEADERS,
  CONSOL_STOP_ITEMS,
  INSTRUCTIONS_INDEX_KEY_NAME,
  IVIA_DDB,
  IVIA_VENDOR_ID,
  // IVIA_CARRIER_ID,
  STAGE,
} = process.env;
const IVIA_CARRIER_ID = "102"; //NOTE:- for stage IVIA need to change it later
const globalConsolIndex = "omni-ivia-ConsolNo-index-" + STAGE;

const loadMultistopConsole = async (dynamoData, shipmentAparData) => {
  console.log("load-Multi-stop-Console");

  const CONSOL_NO = shipmentAparData.ConsolNo;

  /**
   * get data from all the requied tables
   * shipmentApar
   * shipmentInstructions
   * shipmentHeader
   * shipmentDesc
   * consolStopHeaders
   * consolStopItems
   */
  const dataSet = await fetchDataFromTablesList(CONSOL_NO);
  // console.log("dataSet", JSON.stringify(dataSet));

  const shipmentApar = dataSet.shipmentApar;
  const shipmentHeader = dataSet.shipmentHeader;
  const shipmentDesc = dataSet.shipmentDesc;
  const consolStopHeaders = dataSet.consolStopHeaders;
  const consolStopItems = dataSet.consolStopItems;
  const shipmentInstructions = dataSet.shipmentInstructions;

  //only used for liftgate
  const shipmentAparCargo = dataSet.shipmentAparCargo;

  //get all the FK_OrderNo from shipmentApar
  const ORDER_NO_LIST = shipmentApar.map((e) => e.FK_OrderNo);

  let dataArr = [];
  /**
   * merging consolStopItems with consolStopHeaders
   */
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

  /**
   * grouping by pickup type records based on ConsolStopNumber and
   * filtering with ConsolStopPickupOrDelivery === "false"
   */
  const pTypeShipment = groupBy(
    dataArr.filter((e) => e.ConsolStopPickupOrDelivery === "false"),
    "ConsolStopNumber"
  );

  /**
   * grouping by delivery type records based on ConsolStopNumber and
   * filtering with ConsolStopPickupOrDelivery === "true"
   */
  const dTypeShipment = groupBy(
    dataArr.filter((e) => e.ConsolStopPickupOrDelivery === "true"),
    "ConsolStopNumber"
  );

  /**
   * p
   */
  const pTypeShipmentMap = Object.keys(pTypeShipment).map((e) => {
    const ele = pTypeShipment[e];
    const csh = ele[0];

    /**
     * preparing cargo obj form table shipmentDesc
     */
    const cargo = getCargoData(shipmentDesc, ele);

    // prepare notes from shipmentInstructions
    const sInsNotes = shipmentInstructions
      .filter(
        (si) =>
          si.Type.toUpperCase() === "P" && si.FK_OrderNo === csh.FK_OrderNo
      )
      .map((ei) => ei.Note)
      .join(" ");

    /**
     * prepare pickup notes based on consolStopHeader.ConsolStopTimeEnd
     * and consolStopHeader.ConsolStopTimeBegin
     */
    let spInsMsg = "Pickup ";
    spInsMsg +=
      csh.ConsolStopTimeEnd > csh.ConsolStopTimeBegin
        ? "between " +
          moment(csh.ConsolStopTimeBegin).format("HH:mm") +
          " and " +
          moment(csh.ConsolStopTimeEnd).format("HH:mm")
        : "at " + moment(csh.ConsolStopTimeBegin).format("HH:mm");

    /**
     * prepare the Pickup type obj from consolStopHeader
     */
    const stopPayload = {
      stopType: "P",
      stopNum: e,
      housebills: [...new Set(ele.map((e) => e.Housebill))],
      address: {
        address1: csh.ConsolStopAddress1,
        address2: csh.ConsolStopAddress2,
        city: csh.ConsolStopCity,
        country: csh.FK_ConsolStopCountry,
        state: csh.FK_ConsolStopState,
        zip: csh.ConsolStopZip,
      },
      companyName: csh?.ConsolStopName,
      cargo: cargo,
      scheduledDate:
        csh.ConsolStopDate.split(" ")[0] +
        " " +
        (csh.ConsolStopTimeBegin.split(" ")?.[1] ?? ""),
      specialInstructions: (
        spInsMsg +
        "\r\n" +
        csh.ConsolStopNotes +
        "\r\n" +
        sInsNotes
      ).slice(0, 200),
    };
    return stopPayload;
  });

  /**
   * prepare the Delivery type obj from consolStopHeader
   */
  const dTypeShipmentMap = Object.keys(dTypeShipment).map((e) => {
    const ele = dTypeShipment[e];
    const csh = ele[0];

    // prepare notes from shipmentInstructions
    const sInsNotes = shipmentInstructions
      .filter(
        (si) =>
          si.Type.toUpperCase() === "D" && si.FK_OrderNo === csh.FK_OrderNo
      )
      .map((ei) => ei.Note)
      .join(" ");

    /**
     * prepare Delivery notes based on consolStopHeader.ConsolStopTimeEnd
     * and consolStopHeader.ConsolStopTimeBegin
     */
    let spInsMsg = "Deliver ";
    spInsMsg +=
      csh.ConsolStopTimeEnd > csh.ConsolStopTimeBegin
        ? "between " +
          moment(csh.ConsolStopTimeBegin).format("HH:mm") +
          " and " +
          moment(csh.ConsolStopTimeEnd).format("HH:mm")
        : "at " + moment(csh.ConsolStopTimeBegin).format("HH:mm");

    /**
     * prepare the Delivery type obj from consolStopHeader
     */
    const stopPayload = {
      stopType: "D",
      stopNum: e,
      housebills: [...new Set(ele.map((e) => e.Housebill))],
      address: {
        address1: csh.ConsolStopAddress1,
        address2: csh.ConsolStopAddress2,
        city: csh.ConsolStopCity,
        country: csh.FK_ConsolStopCountry,
        state: csh.FK_ConsolStopState,
        zip: csh.ConsolStopZip,
      },
      companyName: csh?.ConsolStopName,
      scheduledDate:
        csh.ConsolStopDate.split(" ")?.[0] +
        " " +
        (csh.ConsolStopTimeBegin.split(" ")?.[1] ?? ""),
      specialInstructions: (
        spInsMsg +
        "\r\n" +
        csh.ConsolStopNotes +
        "\r\n" +
        sInsNotes
      ).slice(0, 200),
    };
    return stopPayload;
  });
  /**
   * merging Pickup and delivery type stopes
   */
  const margedStops = [...pTypeShipmentMap, ...dTypeShipmentMap];
  console.log("margedStops", JSON.stringify(margedStops));

  /**
   * looping all the records and calculating the scheduledDate unix time
   */
  let stopsList = [];
  for (let index = 0; index < margedStops.length; index++) {
    const element = margedStops[index];
    stopsList = [
      ...stopsList,
      {
        ...element,
        scheduledDate: await getGMTDiff(
          element.scheduledDate,
          element.address.zip,
          element.address.country
        ),
      },
    ];
  }

  /**
   * filtered shipmentDesc data based on shipmentApar.FK_OrderNo to get hazardous and unNum
   */
  const filteredSD = shipmentDesc.filter((e) =>
    ORDER_NO_LIST.includes(e.FK_OrderNo)
  );

  const iviaPayload = {
    carrierId: IVIA_CARRIER_ID, // IVIA_CARRIER_ID = 1000025
    refNums: {
      refNum1: CONSOL_NO ?? "", //shipmentApar.ConsolNo
      refNum2: "", //ignore
    },
    shipmentDetails: {
      stops: sortObjByStopNo(stopsList, "stopNum"),
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
      FK_OrderNo: ORDER_NO_LIST.join(","),
      payloadType: "MultistopConsole",
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
 * creates a group of data based on key
 * @param {*} xs
 * @param {*} key
 * @returns
 */
function groupBy(xs, key) {
  return xs.reduce(function (rv, x) {
    (rv[x[key]] = rv[x[key]] || []).push(x);
    return rv;
  }, {});
}

/**
 * cargo data based on shipmentDesc table
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

          if (errorObj.data != JSON.stringify(payload)) {
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
        "FK_VendorId = :FK_VendorId and Consolidation = :Consolidation and FK_ServiceId = :FK_ServiceId and SeqNo <> :SeqNo and FK_OrderNo <> :FK_OrderNo",
      ExpressionAttributeValues: {
        ":ConsolNo": CONSOL_NO.toString(),
        ":FK_VendorId": IVIA_VENDOR_ID.toString(),
        ":Consolidation": "N",
        ":FK_ServiceId": "MT",
        ":SeqNo": "9999",
        ":FK_OrderNo": CONSOL_NO.toString(),
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
      shipmentInstructions,
      shipmentHeader,
      shipmentDesc,
      consolStopHeaders,
      consolStopItems,
      shipmentAparCargo,
    };
  } catch (error) {
    console.log("error", error);
    return {};
  }
}

module.exports = { loadMultistopConsole };
