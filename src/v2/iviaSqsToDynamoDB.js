const AWS = require("aws-sdk");
const { prepareBatchFailureObj } = require("../shared/dataHelper");
const { loadP2PNonConsol } = require("../shared/loadP2PNonConsol");
const { loadP2PConsole } = require("../shared/loadP2PConsole");
const { loadMultistopConsole } = require("../shared/loadMultistopConsole");

const { SHIPMENT_APAR_TABLE } = process.env; //"T19262"

module.exports.handler = async (event, context, callback) => {
  let sqsEventRecords = [];
  try {
    console.log("event", JSON.stringify(event));
    sqsEventRecords = event.Records;
    // sqsEventRecords = [{}];

    const faildSqsItemList = [];

    for (let index = 0; index < sqsEventRecords.length; index++) {
      try {
        const sqsItem = sqsEventRecords[index];
        const dynamoData = JSON.parse(sqsItem.body);
        // const dynamoData = {
        //   ApproximateCreationDateTime: 1676961662,
        //   Keys: { SeqNo: { S: "1" }, FK_OrderNo: { S: "4917047" } },
        //   NewImage: {
        //     APARCode: { S: "V" },
        //     Finalize: { S: "N" },
        //     InvoiceSeqNo: { S: "0" },
        //     FinalizedDate: { S: "1900-01-01 00:00:00.000" },
        //     FK_AirCode: { S: "" },
        //     ReadyForInvoice: { S: "N" },
        //     FK_VendorId: { S: "T19262" },
        //     Tax: { S: "0.00" },
        //     FK_HandlingStation: { S: "" },
        //     UpdatedBy: { S: "wwaller" },
        //     APARStatus: { S: "" },
        //     Currency: { S: "USD" },
        //     UpdatedOn: { S: "2023-02-20 14:26:17.733" },
        //     VendorAmount: { S: "0.00" },
        //     Override: { S: "O" },
        //     SeqNo: { S: "1" },
        //     FK_PaymentTermCode: { S: "NULL" },
        //     FK_ConsolStationId: { S: "" },
        //     FK_ServiceId: { S: "HS" },
        //     CustomerInvoiceNo: { S: "" },
        //     Rate: { S: "0.0000" },
        //     RefNo: { S: "9200006A" },
        //     Weight: { S: "0.0" },
        //     ChargeCode: { S: "" },
        //     FK_EquipmentCode: { S: "NULL" },
        //     FK_ContainerCode: { S: "" },
        //     FK_PaymentTermCode1: { S: "NULL" },
        //     Description: { S: "" },
        //     Complete: { S: "N" },
        //     FK_ConsolStatusId: { S: "" },
        //     CreateDateTime: { S: "2022-12-29 14:53:33.173" },
        //     FinalizedBy: { S: "" },
        //     Cost: { S: "650.00" },
        //     ConsolShipDateTime: { S: "1900-01-01 00:00:00.000" },
        //     APARType: { S: "CLB" },
        //     ConsolNo: { S: "0" },
        //     ReadyForInvoiceDateTime: { S: "1900-01-01 00:00:00.000" },
        //     Consolidation: { S: "N" },
        //     FK_AccountCode: { S: "" },
        //     FK_CodeNo: { S: "0" },
        //     FK_CustNo: { S: "19197" },
        //     VendorCostSeqNo: { S: "0" },
        //     InvoiceDate: { S: "1900-01-01 00:00:00.000" },
        //     DueDate: { S: "1900-01-01 00:00:00.000" },
        //     InvPrinted: { S: "N" },
        //     FK_OrderNo: { S: "4917047" },
        //     Quantity: { S: "57.000" },
        //     InvoiceNo: { S: "" },
        //     PKSeqNo: { S: "40942588" },
        //     PostedDateTime: { S: "1900-01-01 00:00:00.000" },
        //     InvPrintedDate: { S: "1900-01-01 00:00:00.000" },
        //     Extra: { S: "0.00" },
        //     FinalizedTotal: { S: "0.00" },
        //     InsertedTimeStamp: { S: "2023:02:20 14:27:14" },
        //     Total: { S: "650.00" },
        //   },
        //   SequenceNumber: "875323600000000000435155807",
        //   SizeBytes: 1933,
        //   StreamViewType: "NEW_AND_OLD_IMAGES",
        //   dynamoTableName: "omni-wt-rt-shipment-apar-dev",
        // };
        if (dynamoData.dynamoTableName !== SHIPMENT_APAR_TABLE) {
          continue;
        }

        const shipmentAparData = AWS.DynamoDB.Converter.unmarshall(
          dynamoData.NewImage
        );

        if (shipmentAparData.ConsolNo === "0") {
          /**
           * loadP2PNonConsol
           */
          await loadP2PNonConsol(dynamoData, shipmentAparData);
        } else if (parseInt(shipmentAparData.ConsolNo) > 0) {
          if (["HS", "TL"].includes(shipmentAparData?.FK_ServiceId)) {
            /**
             * loadP2PConsole
             */
            await loadP2PConsole(dynamoData, shipmentAparData);
          } else if (shipmentAparData.FK_ServiceId === "MT") {
            /**
             * loadMultistopConsole
             */
            await loadMultistopConsole(dynamoData, shipmentAparData);
          } else {
            //exception
            console.log("exception consol> 0");
            throw "exception";
          }
        } else {
          console.log("exception global");
          throw "exception";
        }
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
