const AWS = require("aws-sdk");
const { v4: uuidv4 } = require("uuid");
const moment = require("moment");
const momentTZ = require("moment-timezone");
const { prepareBatchFailureObj } = require("../shared/dataHelper");
const {
  queryWithPartitionKey,
  queryWithIndex,
  putItem,
} = require("../shared/dynamo");

const SHIPMENT_APAR_TABLE = process.env.SHIPMENT_APAR_TABLE; //"T19262"
const SHIPMENT_HEADER_TABLE = process.env.SHIPMENT_HEADER_TABLE;
const CONSIGNEE_TABLE = process.env.CONSIGNEE_TABLE;
const SHIPPER_TABLE = process.env.SHIPPER_TABLE;
const INSTRUCTIONS_TABLE = process.env.INSTRUCTIONS_TABLE;
const SHIPMENT_DESC_TABLE = process.env.SHIPMENT_DESC_TABLE;

const CONFIRMATION_COST = process.env.CONFIRMATION_COST;
const CONSOL_STOP_HEADERS = process.env.CONSOL_STOP_HEADERS;
const CONSOL_STOP_ITEMS = process.env.CONSOL_STOP_ITEMS;

const CONFIRMATION_COST_INDEX_KEY_NAME =
  process.env.CONFIRMATION_COST_INDEX_KEY_NAME;

const INSTRUCTIONS_INDEX_KEY_NAME = process.env.INSTRUCTIONS_INDEX_KEY_NAME;
const IVIA_DDB = process.env.IVIA_DDB;
const IVIA_CARRIER_ID = process.env.IVIA_CARRIER_ID;
const IVIA_VENDOR_ID = process.env.IVIA_VENDOR_ID;

module.exports.handler = async (event, context, callback) => {
  let sqsEventRecords = [];
  try {
    console.log("event", JSON.stringify(event));
    sqsEventRecords = event.Records;

    const faildSqsItemList = [];

    for (let index = 0; index < sqsEventRecords.length; index++) {
      try {
        const sqsItem = sqsEventRecords[index];
        const dynamoData = JSON.parse(sqsItem.body);

        //get the primary key
        const { tableList, primaryKeyValue } = getTablesAndPrimaryKey(
          dynamoData.dynamoTableName,
          dynamoData
        );

        //get data from all the requied tables
        const dataSet = await fetchDataFromTables(tableList, primaryKeyValue);
        console.log("dataSet", JSON.stringify(dataSet));
        let shipmentAparData = dataSet.shipmentApar
          .filter((e) => e.FK_VendorId === IVIA_VENDOR_ID)
          .reduce((a, b) => {
            return a.SeqNo > b.SeqNo ? a : b;
          });

        //if got multiple data , take latest one based on seqNo and time.
        shipmentAparData =
          dynamoData.dynamoTableName === SHIPMENT_APAR_TABLE
            ? AWS.DynamoDB.Converter.unmarshall(dynamoData.NewImage)
            : shipmentAparData;
        // console.log("shipmentAparData", shipmentAparData);

        if (shipmentAparData?.FK_VendorId != IVIA_VENDOR_ID) {
          continue;
        }

        let iviaPayload = "";

        if (["HS", "TL"].includes(shipmentAparData?.FK_ServiceId)) {
          if (dataSet.confirmationCost?.[0]?.ConsolNo === "0") {
            // payload 2 way starts // non consol p2p // send housebill no
            // exactly one shipfrom/ to address in tbl_confirmation_cost for file number
            if (dataSet.confirmationCost.length === 1) {
              //payload 2
              iviaPayload = loadP2PNonConsol(dataSet, shipmentAparData);
            } else {
              //exception payload 2
              console.log("exception payload 2");
              throw "exception payload 2";
            }
          } else {
            //one ship from/ to address in tbl_confiramation_cost for consol number
            if (
              dataSet.confirmationCost[0].ConsolNo > 0 &&
              dataSet.confirmationCost.length === 1
            ) {
              // payload 3 way starts // send consol_no  //   p2p consol
              // pieces weight dims for each housebill in consol
              iviaPayload = loadP2PConsole(dataSet, shipmentAparData);
            } else {
              //exception
              console.log("exception payload 2");
              throw "exception payload 3";
            }
          }
        } else if (shipmentAparData.FK_ServiceId === "MT") {
          // payload 1 way starts // send consol_no // multistop consol
          // exectly one pickup and one delivery address in consol stop headers for each housebill
          if (dataSet.consolStopHeaders.length === 1) {
            iviaPayload = loadMultistopConsole(dataSet, shipmentAparData);
          } else {
            //exception
            console.log("exception payload 1");
            throw "exception payload 1";
          }
        } else {
          //exception
          console.log("exception ");
          throw "exception";
        }

        //prepare the payload
        console.log("iviaObj", JSON.stringify(iviaPayload));

        //save to dynamo DB
        await putItem(IVIA_DDB, {
          id: uuidv4(),
          data: JSON.stringify(iviaPayload),
          Housebill: shipmentHeader?.[0]?.Housebill,
          ConsolNo: shipmentAparData?.ConsolNo,
          FK_OrderNo: shipmentAparData?.FK_OrderNo,
          InsertedTimeStamp: momentTZ
            .tz("America/Chicago")
            .format("YYYY:MM:DD HH:mm:ss")
            .toString(),
        });
      } catch (error) {
        console.log("error", error);
      }
    }
    return prepareBatchFailureObj(faildSqsItemList);
  } catch (error) {
    console.error("Error", error);
    return prepareBatchFailureObj(sqsEventRecords);
  }
};

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
      [INSTRUCTIONS_TABLE]: {
        PK: "PK_InstructionNo",
        SK: "",
        sortName: "shipmentInstructions",
        indexKeyColumnName: "FK_OrderNo",
        indexKeyName: INSTRUCTIONS_INDEX_KEY_NAME, //"omni-wt-instructions-orderNo-index-{stage}"
        type: "INDEX",
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
      [CONSOL_STOP_ITEMS]: {
        PK: "FK_OrderNo",
        SK: "FK_ConsolStopId",
        sortName: "consolStopItems",
        type: "PRIMARY_KEY",
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

    //fetch consolStopHeaders
    let consolStopHeaderData = [];
    for (let index = 0; index < newObj.consolStopItems.length; index++) {
      const element = newObj.consolStopItems[index];
      const data = await queryWithPartitionKey(CONSOL_STOP_HEADERS, {
        PK_ConsolStopId: element.FK_ConsolStopId,
      });
      consolStopHeaderData = [...consolStopHeaderData, ...data.Items];
    }
    newObj["consolStopHeaders"] = consolStopHeaderData;
    return newObj;
  } catch (error) {
    console.log("error:fetchDataFromTables", error);
  }
}

/**
 * non console p2p // send housebill no
 */
const loadP2PNonConsol = (dataSet, shipmentAparData) => {
  console.log("loadP2PNonConsol");
  const confirmationCost = dataSet.confirmationCost;
  const shipmentApar = shipmentAparData;
  const shipmentHeader = dataSet.shipmentHeader;

  const shipmentDesc = dataSet.shipmentDesc.filter((e) => e.ConsolNo === "0");

  const filteredConfirmationCost = confirmationCost.filter((e) => {
    return (
      e.FK_OrderNo === shipmentApar.FK_OrderNo &&
      e.FK_SeqNo === shipmentApar.SeqNo &&
      shipmentApar.Consolidation === "N"
    );
  });

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
        e.ConAddress2 === "" ? "" : e.ConAddress2 + " " + e.PickupNote,
    };
  });

  const iviaPayload = {
    carrierId: IVIA_CARRIER_ID, // IVIA_CARRIER_ID = 1000025
    refNums: {
      refNum1: dataSet.shipmentHeader[0].PK_OrderNo ?? "", // tbl_shipmentHeader.pk_orderNo as hwb/ tbl_confirmationCost.consolNo(if it is a consol)
      refNum2: confirmationCost[0].FK_OrderNo ?? "", // tbl_confirmationcost.fk_orderno as filenumber
    },
    shipmentDetails: {
      stops: [...pStopTypeData, ...dStopTypeData],
      dockHigh: "N", // req [Y / N]
      hazardous: shipmentDesc?.Hazmat ?? "N",
      liftGate: getLiftGate(shipmentApar?.ChargeCode ?? ""),
      unNum: getUnNum(shipmentDesc), // accepts only 4 degit number as string
      // notes: getNotes(shipmentInstructions, "S"),
    },
  };
  console.log("iviaPayload", JSON.stringify(iviaPayload));
  return iviaPayload;
};

/**
 * point to point console // send console no
 */
const loadP2PConsole = (dataSet, shipmentAparData) => {
  console.log("loadP2PConsole");
  const confirmationCost = dataSet.confirmationCost;
  const shipmentApar = shipmentAparData;
  const shipmentHeader = dataSet.shipmentHeader;

  const filteredConfirmationCost = confirmationCost.filter((e) => {
    return (
      e.FK_OrderNo === shipmentApar.FK_OrderNo &&
      e.FK_SeqNo === shipmentApar.SeqNo &&
      shipmentApar.Consolidation === "N"
    );
  });

  const shipmentDesc = dataSet.shipmentDesc.filter(
    (e) =>
      e.ConsolNo === shipmentApar.ConsolNo && shipmentApar.Consolidation === "N"
  );

  const filteredOrderNoList = filteredConfirmationCost
    .filter((e) => e.FK_SeqNo < 9999)
    .map((e) => e.FK_OrderNo);
  console.log(
    "filteredOrderNoList",
    filteredOrderNoList.length,
    filteredOrderNoList[0]
  );
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
        e.ConAddress2 === "" ? "" : e.ConAddress2 + " " + e.PickupNote,
    };
  });

  const iviaPayload = {
    carrierId: IVIA_CARRIER_ID, // IVIA_CARRIER_ID = 1000025
    refNums: {
      refNum1: filteredConfirmationCost[0].ConsolNo ?? "", // tbl_shipmentHeader.pk_orderNo as hwb/ tbl_confirmationCost.consolNo(if it is a consol)
      refNum2: "", // as query filenumber value is always ""
    },
    shipmentDetails: {
      stops: [...pStopTypeData, ...dStopTypeData],
      dockHigh: "N", // req [Y / N]
      hazardous: shipmentDesc?.Hazmat ?? "N",
      liftGate: getLiftGate(shipmentApar?.ChargeCode ?? ""),
      unNum: getUnNum(shipmentDesc.shipmentDesc), // accepts only 4 degit number as string
      // notes: getNotes(shipmentInstructions, "S"),
    },
  };
  console.log("iviaPayload", JSON.stringify(iviaPayload));
  return iviaPayload;
};

/**
 * multistop console //non consol p2p // send console no
 */
const loadMultistopConsole = (dataSet, shipmentAparData) => {
  console.log("loadMultistopConsole");
  const confirmationCost = dataSet.confirmationCost;
  const consolStopHeaders = dataSet.consolStopHeaders;
  const consolStopItems = dataSet.consolStopItems;
  const shipmentApar = shipmentAparData;
  const shipmentHeader = dataSet.shipmentHeader;

  const filteredConfirmationCost = confirmationCost.filter((e) => {
    return (
      e.FK_OrderNo === shipmentApar.FK_OrderNo &&
      e.FK_SeqNo === shipmentApar.SeqNo &&
      shipmentApar.Consolidation === "N"
    );
  });

  const housebill_delimited = shipmentHeader
    .filter((e) => {
      const conHeaders = consolStopHeaders
        .filter(
          (e) =>
            e.FK_ConsolNo === shipmentApar.ConsolNo && e.ConsolStopNumber === 0
        )
        .map((e) => e.PK_ConsolStopId);
      const orderNoList = consolStopItems
        .filter((e) => conHeaders.includes(e.FK_ConsolStopId))
        .map((e) => e.FK_OrderNo);
      return orderNoList.includes(e.PK_OrderNo);
    })
    .map((e) => e.Housebill);

  const cargo = dataSet.shipmentDesc
    .filter((e) => {
      const conHeaders = consolStopHeaders
        .filter(
          (e) =>
            e.FK_ConsolNo === shipmentApar.ConsolNo && e.ConsolStopNumber === 0
        )
        .map((e) => e.PK_ConsolStopId);
      const orderNoList = consolStopItems
        .filter((e) => conHeaders.includes(e.FK_ConsolStopId))
        .map((e) => e.FK_OrderNo);
      return orderNoList.includes(e.FK_OrderNo);
    })
    .map((e) => {
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

  const shipmentDetailsStops = consolStopItems
    .filter(
      (e) =>
        shipmentApar.Consolidation === "N" &&
        e.FK_ConsolNo === shipmentApar.ConsolNo
    )
    .map((e) => {
      //for every consolStopItems we will have only one consoleStopHeader data
      const csh =
        consolStopHeaders.filter(
          (e_csh) => e_csh.PK_ConsolStopId === e.FK_ConsolStopId
        )?.[0] ?? {};

      let shipmentInstructions =
        dataSet.shipmentInstructions.length > 0
          ? dataSet.shipmentInstructions
          : [];

      //ConsolStopPickupOrDelivery (false = P, true = D)
      if (csh.ConsolStopPickupOrDelivery === "false") {
        // filter based on type
        shipmentInstructions = shipmentInstructions
          .filter((e) => {
            return e.Type === "P";
          })
          .map((e) => e.Note);
        shipmentInstructions =
          shipmentInstructions.length > 0 ? shipmentInstructions.join(" ") : "";
        return {
          stopType: "P",
          stopNum: 0,
          housebills: housebill_delimited,
          address: {
            address1: e.ConsolStopAddress1,
            city: e.ConsolStopCity,
            country: e.FK_ConsolStopCountry,
            state: e.FK_ConsolStopState,
            zip: e.ConsolStopZip,
          },
          companyName: csh?.ConsolStopName,
          cargo: cargo,
          scheduledDate: moment(
            e.ConsolStopDate.split(" ")[0] +
              " " +
              e.ConsolStopTimeBegin.split(" ")[1]
          ).diff("1970-01-01", "ms"),
          specialInstructions:
            (e.ConsolStopAddress2 === "" ? "" : e.ConsolStopAddress2 + " ") +
            shipmentInstructions,
        };
      } else {
        shipmentInstructions = shipmentInstructions
          .filter((e) => {
            return e.Type === "D";
          })
          .map((e) => e.Note);
        shipmentInstructions =
          shipmentInstructions.length > 0 ? shipmentInstructions.join(" ") : "";
        return {
          stopType: "D",
          stopNum: 1,
          housebills: housebill_delimited,
          address: {
            address1: e.ConsolStopAddress1,
            city: e.ConsolStopCity,
            country: e.FK_ConsolStopCountry,
            state: e.FK_ConsolStopState,
            zip: e.ConsolStopZip,
          },
          companyName: csh?.ConsolStopName,
          scheduledDate: moment(
            e.ConsolStopDate.split(" ")[0] +
              " " +
              e.ConsolStopTimeBegin.split(" ")[1]
          ).diff("1970-01-01", "ms"),
          specialInstructions:
            (e.ConsolStopAddress2 === "" ? "" : e.ConsolStopAddress2 + " ") +
            shipmentInstructions,
        };
      }
    });

  const iviaPayload = {
    carrierId: IVIA_CARRIER_ID, // IVIA_CARRIER_ID = 1000025
    refNums: {
      refNum1: filteredConfirmationCost[0].ConsolNo ?? "", // tbl_shipmentHeader.pk_orderNo as hwb/ tbl_confirmationCost.consolNo(if it is a consol)
      refNum2: "", // as query filenumber value is always "" hardcode
    },
    shipmentDetails: {
      stops: shipmentDetailsStops,
      dockHigh: "N", // req [Y / N]
      hazardous: shipmentDesc?.Hazmat ?? "N",
      liftGate: getLiftGate(shipmentApar?.ChargeCode ?? ""),
      unNum: getUnNum(shipmentDesc.shipmentDesc), // accepts only 4 degit number as string
    },
  };
  console.log("iviaPayload", JSON.stringify(iviaPayload));
  return iviaPayload;
};

/**
 * if we got multiple records from one table then we are taking the latest one.
 * @param {*} data
 * @returns
 */
function getLatestObjByTimeStamp(data) {
  if (data.length > 1) {
    return data.sort((a, b) => {
      let atime = a.InsertedTimeStamp.split(" ");
      atime = atime[0].split(":").join("-") + " " + atime[1];

      let btime = b.InsertedTimeStamp.split(" ");
      btime = btime[0].split(":").join("-") + " " + btime[1];

      return new Date(btime) - new Date(atime);
    })[0];
  } else if (data.length === 1) {
    return data[0];
  } else {
    return {};
  }
}

// function getValidDate(date) {
//   try {
//     if (moment(date).isValid() && !date.includes("1970")) {
//       return new Date(date).getTime();
//     } else {
//       return 0;
//     }
//   } catch (error) {
//     return 0;
//   }
// }

/**
 * get "Y" or "N" based on available lift gate
 * @param {*} param
 * @returns
 */
function getLiftGate(param) {
  try {
    if (["LIFT", "LIFTD", "LIFTP", "TRLPJ"].includes(param.toUpperCase())) {
      console.log("param", param);
      return "Y";
    } else {
      return "N";
    }
  } catch (error) {
    return "N";
  }
}

// function getNotes(data, type) {
//   try {
//     return data
//       .filter((e) => e.Type.toUpperCase() === type.toUpperCase())
//       .map((e) => e.Note)
//       .join(",");
//   } catch (error) {
//     return "";
//   }
// }

/**
 * unNum is a number with length 4 and it value should be 0001 to 3600
 * we populate this field if we have "Hazmat" = "Y"
 * example  "UN 2234 ST 1234" so we are taking 2234 as unNum
 * @param {*} param
 * @returns
 */
function getUnNum(param) {
  try {
    const data = param.filter((e) => e.Hazmat.toUpperCase() === "Y");
    const obj = data.length > 0 ? getLatestObjByTimeStamp(data) : {};
    const unArr = obj.description.split(" ");
    if (unArr[0] === "UN") {
      return unArr.filter((e, i) => {
        return (
          i <= 2 && e.length === 4 && parseInt(e) >= 1 && parseInt(e) <= 3600
        );
      })[0];
    }
    return "";
  } catch (error) {
    return "";
  }
}
