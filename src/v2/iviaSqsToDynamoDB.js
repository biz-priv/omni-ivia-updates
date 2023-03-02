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

      //if same event multiple line
    }
    return prepareBatchFailureObj(faildSqsItemList);
  } catch (error) {
    console.error("Error", error);
    return prepareBatchFailureObj(sqsEventRecords);
  }
};
