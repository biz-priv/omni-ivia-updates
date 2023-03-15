const AWS = require("aws-sdk");
const { prepareBatchFailureObj, setDelay } = require("../shared/dataHelper");
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
    //NOTE :- delay 45

    for (let index = 0; index < sqsEventRecords.length; index++) {
      try {
        await setDelay(45);
        const sqsItem = sqsEventRecords[index];
        const dynamoData = JSON.parse(sqsItem.body);
        // const dynamoData = {
        //   ApproximateCreationDateTime: 1673976738,
        //   Keys: { SeqNo: { S: "1" }, FK_OrderNo: { S: "4931065" } },
        //   NewImage: {
        //     FK_OrderNo: {
        //       S: "4929262",
        //     },
        //     SeqNo: {
        //       S: "2",
        //     },
        //     APARCode: {
        //       S: "V",
        //     },
        //     APARStatus: {
        //       S: "",
        //     },
        //     APARType: {
        //       S: "CLB",
        //     },
        //     ChargeCode: {
        //       S: "",
        //     },
        //     Consolidation: {
        //       S: "N",
        //     },
        //     ConsolNo: {
        //       S: "304897",
        //     },
        //     ConsolShipDateTime: {
        //       S: "1900-01-01 00:00:00.000",
        //     },
        //     Cost: {
        //       S: "0.00",
        //     },
        //     CreateDateTime: {
        //       S: "2023-03-13 13:50:21.000",
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
        //       S: "1900-01-01 00:00:00.000",
        //     },
        //     Extra: {
        //       S: "0.00",
        //     },
        //     Finalize: {
        //       S: "N",
        //     },
        //     FinalizedBy: {
        //       S: "",
        //     },
        //     FinalizedDate: {
        //       S: "1900-01-01 00:00:00.000",
        //     },
        //     FinalizedTotal: {
        //       S: "0.00",
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
        //       S: "PHX",
        //     },
        //     FK_ConsolStatusId: {
        //       S: "",
        //     },
        //     FK_ContainerCode: {
        //       S: "D",
        //     },
        //     FK_CustNo: {
        //       S: "19197",
        //     },
        //     FK_EquipmentCode: {
        //       S: "NULL",
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
        //       S: "MT",
        //     },
        //     FK_VendorId: {
        //       S: "T19262",
        //     },
        //     InsertedTimeStamp: {
        //       S: "2023:03:13 14:38:08",
        //     },
        //     InvoiceDate: {
        //       S: "1900-01-01 00:00:00.000",
        //     },
        //     InvoiceNo: {
        //       S: "",
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
        //       S: "",
        //     },
        //     PKSeqNo: {
        //       S: "40952455",
        //     },
        //     PostedDateTime: {
        //       S: "1900-01-01 00:00:00.000",
        //     },
        //     Quantity: {
        //       S: "25000.000",
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
        //       S: "304897",
        //     },
        //     Tax: {
        //       S: "0.00",
        //     },
        //     Total: {
        //       S: "0.00",
        //     },
        //     UpdatedBy: {
        //       S: "wwaller",
        //     },
        //     UpdatedOn: {
        //       S: "2023-03-13 14:37:06.000",
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
        } else if (
          parseInt(shipmentAparData.ConsolNo) > 0 &&
          parseInt(shipmentAparData.SeqNo) < 9999
        ) {
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
            console.log(
              "exception consol> 0 and shipmentApar.FK_ServiceId is not in HS/TL/MT "
            );
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
