const AWS = require("aws-sdk");
const { v4: uuidv4 } = require("uuid");
const moment = require("moment");
const momentTZ = require("moment-timezone");
const { prepareBatchFailureObj } = require("./shared/dataHelper");
const {
  getItem,
  queryWithPartitionKey,
  queryWithIndex,
  putItem,
} = require("./shared/dynamo");

const SHIPMENT_APAR_TABLE = process.env.SHIPMENT_APAR_TABLE;
const SHIPMENT_HEADER_TABLE = process.env.SHIPMENT_HEADER_TABLE;
const CONSIGNEE_TABLE = process.env.CONSIGNEE_TABLE;
const SHIPPER_TABLE = process.env.SHIPPER_TABLE;
const INSTRUCTIONS_TABLE = process.env.INSTRUCTIONS_TABLE;
const SHIPMENT_DESC_TABLE = process.env.SHIPMENT_DESC_TABLE;
const INSTRUCTIONS_INDEX_KEY_NAME = process.env.INSTRUCTIONS_INDEX_KEY_NAME;
// const IVIA_DDB = process.env.IVIA_DDB;
const IVIA_DDB = "omni-rt-ivia-dev";
// const INSTRUCTIONS_INDEX_KEY_NAME = "omni-wt-instructions-orderNo-index-dev";

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
        // const dynamoData = sqsItem.body;
        // console.log("dynamoData", dynamoData);

        //get the primary key
        const { tableList, primaryKeyValue } = getTablesAndPrimaryKey(
          dynamoData.dynamoTableName,
          dynamoData
        );
        const shipmentAparData = AWS.DynamoDB.Converter.unmarshall(
          dynamoData.NewImage
        );
        //get data from all the requied tables
        const dataSet = await fetchDataFromTables(tableList, primaryKeyValue);

        //prepare the payload
        const iviaObj = mapIviaData(dataSet, shipmentAparData);
        console.log("iviaObj", JSON.stringify(iviaObj));

        //save to dynamo DB
        await putItem(IVIA_DDB, {
          id: uuidv4(),
          data: JSON.stringify(iviaObj.iviaData),
          Housebill: iviaObj.Housebill,
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
        indexKeyName: INSTRUCTIONS_INDEX_KEY_NAME, //"omni-wt-instructions-orderNo-index-dev"
        type: "INDEX",
      },
      [SHIPMENT_DESC_TABLE]: {
        PK: "FK_OrderNo",
        SK: "SeqNo",
        sortName: "shipmentDesc",
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
    console.info("error:unable to select table", error);
    console.info("tableName", tableName);
    throw error;
  }
}

async function fetchDataFromTables(tableList, primaryKeyValue) {
  try {
    const data = await Promise.all(
      Object.keys(tableList).map(async (e) => {
        const tableName = e;
        const ele = tableList[tableName];
        let data = [];

        if (ele.type === "INDEX") {
          console.log(tableName, ele);
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
    console.log("data", data);
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

function mapIviaData(dataSet, shipmentAparData) {
  try {
    // console.log("dataSet", dataSet);
    // shipmentHeader,consignee,shipper,shipmentApar always have one value

    const shipmentHeader =
      dataSet.shipmentHeader.length > 0 ? dataSet.shipmentHeader[0] : {};
    const consignee = dataSet.consignee.length > 0 ? dataSet.consignee[0] : {};
    const shipper = dataSet.shipper.length > 0 ? dataSet.shipper[0] : {};
    const shipmentApar = shipmentAparData;

    const shipmentInstructions =
      dataSet.shipmentInstructions.length > 0
        ? dataSet.shipmentInstructions
        : [];
    const shipmentDesc = getLatestObjByTimeStamp(dataSet.shipmentDesc);

    const iviaPayload = {
      carrierId: 1004451, //hardcode
      refNums: {
        refNum1: shipmentHeader?.Housebill,
        refNum2: shipmentHeader?.PK_OrderNo,
        refNum3: shipmentApar?.ConsolNo,
      },
      shipmentDetails: {
        destination: {
          address: {
            address1: consignee?.ConAddress1 ?? "",
            city: consignee?.ConCity ?? "",
            country: consignee?.FK_ConCountry ?? "",
            state: consignee?.FK_ConState ?? "",
            zip: consignee?.ConZip ?? "",
          },
          companyName: consignee?.ConName ?? "",
          scheduledDate: getValidDate(shipmentHeader?.ScheduledDateTime),
          specialInstructions: getNotes(shipmentInstructions, "D"),
        },
        dockHigh: "N", // req [Y / N]
        hazardous: shipmentDesc?.Hazmat ?? "N",
        liftGate: getLiftGate(shipmentApar?.ChargeCode ?? ""),
        notes: getNotes(shipmentInstructions, "S"),
        origin: {
          address: {
            address1: shipper?.ShipAddress1 ?? "",
            city: shipper?.ShipCity ?? "",
            country: shipper?.FK_ShipCountry ?? "",
            state: shipper?.FK_ShipState ?? "",
            zip: shipper?.ShipZip ?? "",
          },
          cargo: [
            {
              height: shipmentDesc?.Height ?? "",
              length: shipmentDesc?.Length ?? "",
              packageType: "PIE",
              quantity: shipmentDesc?.Pieces ?? "", //req
              stackable: "N", // req [Y / N]
              turnable: "N", // req [Y / N]
              weight: shipmentDesc?.Weight ?? "", //req
              width: shipmentDesc?.Width ?? "",
            },
          ],
          companyName: shipper?.ShipName,
          scheduledDate: getValidDate(shipmentHeader?.ReadyDateTime),
          specialInstructions: getNotes(shipmentInstructions, "P"),
        },
        unNum: getUnNum(dataSet.shipmentDesc), // accepts only 4 degit number as string
      },
    };
    return {
      iviaData: iviaPayload,
      Housebill: shipmentHeader?.Housebill ?? "",
    };
  } catch (error) {
    console.log("error:mapIviaData", error);
    throw "Error creating payload";
  }
}

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

function getValidDate(date) {
  try {
    if (moment(date).isValid() && !date.includes("1970")) {
      return new Date(date).getTime();
    } else {
      return 0;
    }
  } catch (error) {
    return 0;
  }
}

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

function getNotes(data, type) {
  try {
    return data
      .filter((e) => e.Type.toUpperCase() === type.toUpperCase())
      .map((e) => e.Note)
      .join(",");
  } catch (error) {
    return "";
  }
}

function getUnNum(param) {
  try {
    const data = param.filter((e) => e.Hazmat.toUpperCase() === "Y");
    const obj = data.length > 0 ? getLatestObjByTimeStamp(data) : {};
    const unArr = obj.split(" ");
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
