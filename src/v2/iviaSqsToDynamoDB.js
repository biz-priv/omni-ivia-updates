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
        //   ApproximateCreationDateTime: 1673976738,
        //   Keys: { SeqNo: { S: "9999" }, FK_OrderNo: { S: "184503" } },
        //   NewImage: {
        //     FK_OrderNo: {
        //       S: "184503",
        //     },
        //     SeqNo: {
        //       S: "9999",
        //     },
        //     APARCode: {
        //       S: "V",
        //     },
        //     APARStatus: {
        //       S: "ok123",
        //     },
        //     APARType: {
        //       S: "CLB",
        //     },
        //     ChargeCode: {
        //       S: "",
        //     },
        //     Complete: {
        //       S: "Y",
        //     },
        //     Consolidation: {
        //       S: "Y",
        //     },
        //     ConsolNo: {
        //       S: "184503",
        //     },
        //     ConsolShipDateTime: {
        //       S: "2020-08-11 17:43:00.000",
        //     },
        //     Cost: {
        //       S: "350.00",
        //     },
        //     CreateDateTime: {
        //       S: "2020-08-11 17:44:54.000",
        //     },
        //     Currency: {
        //       S: "USD",
        //     },
        //     CustomerInvoiceNo: {
        //       S: "",
        //     },
        //     Description: {
        //       S: "",
        //     },
        //     DueDate: {
        //       S: "2020-09-12 00:00:00.000",
        //     },
        //     Extra: {
        //       S: "0.00",
        //     },
        //     Finalize: {
        //       S: "Y",
        //     },
        //     FinalizedBy: {
        //       S: "rramos",
        //     },
        //     FinalizedDate: {
        //       S: "2020-09-21 14:25:34.000",
        //     },
        //     FinalizedTotal: {
        //       S: "350.00",
        //     },
        //     FK_AccountCode: {
        //       S: "",
        //     },
        //     FK_AirCode: {
        //       S: "",
        //     },
        //     FK_CodeNo: {
        //       S: "0",
        //     },
        //     FK_ConsolStationId: {
        //       S: "OTR",
        //     },
        //     FK_ConsolStatusId: {
        //       S: "CDE",
        //     },
        //     FK_ContainerCode: {
        //       S: "T",
        //     },
        //     FK_CustNo: {
        //       S: "19197",
        //     },
        //     FK_EquipmentCode: {
        //       S: "SM SPRINT",
        //     },
        //     FK_HandlingStation: {
        //       S: "",
        //     },
        //     FK_PaymentTermCode: {
        //       S: "NULL",
        //     },
        //     FK_PaymentTermCode1: {
        //       S: "NULL",
        //     },
        //     FK_ServiceId: {
        //       S: "HS",
        //     },
        //     FK_VendorId: {
        //       S: "T19262",
        //     },
        //     InsertedTimeStamp: {
        //       S: "2022:12:12 05:07:18",
        //     },
        //     InvoiceDate: {
        //       S: "2020-08-13 00:00:00.000",
        //     },
        //     InvoiceNo: {
        //       S: "4950780A",
        //     },
        //     InvoiceSeqNo: {
        //       S: "0",
        //     },
        //     InvPrinted: {
        //       S: "N",
        //     },
        //     InvPrintedDate: {
        //       S: "1900-01-01 00:00:00.000",
        //     },
        //     Override: {
        //       S: "O",
        //     },
        //     PKSeqNo: {
        //       S: "24595545",
        //     },
        //     PostedDateTime: {
        //       S: "2020-09-22 12:03:50.000",
        //     },
        //     Quantity: {
        //       S: "119.000",
        //     },
        //     Rate: {
        //       S: "0.0000",
        //     },
        //     ReadyForInvoice: {
        //       S: "N",
        //     },
        //     ReadyForInvoiceDateTime: {
        //       S: "1900-01-01 00:00:00.000",
        //     },
        //     RefNo: {
        //       S: "184503",
        //     },
        //     Tax: {
        //       S: "0.00",
        //     },
        //     Total: {
        //       S: "350.00",
        //     },
        //     UpdatedBy: {
        //       S: "vbibi",
        //     },
        //     UpdatedOn: {
        //       S: "2020-09-22 12:03:50.000",
        //     },
        //     VendorAmount: {
        //       S: "0.00",
        //     },
        //     VendorCostSeqNo: {
        //       S: "0",
        //     },
        //     Weight: {
        //       S: "0.0",
        //     },
        //   },
        //   SequenceNumber: "426688000000000028803607296",
        //   SizeBytes: 991,
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
