/*
* File: src\v2\iviaSqsToDynamoDB.js
* Project: Omni-ivia-updates
* Author: Bizcloud Experts
* Date: 2023-03-24
* Confidential and Proprietary
*/
const AWS = require("aws-sdk");
const { prepareBatchFailureObj, setDelay } = require("../shared/dataHelper");
const { loadP2PNonConsol } = require("../shared/loadP2PNonConsol");
const { loadP2PConsole } = require("../shared/loadP2PConsole");
const { loadMultistopConsole } = require("../shared/loadMultistopConsole");

const { SHIPMENT_APAR_TABLE } = process.env; //"T19262"

module.exports.handler = async (event, context, callback) => {
  //TODO:- stop stage events
  if (process.env.STAGE.toLowerCase() === "stg") {
    return "Success";
  }
  let sqsEventRecords = [];
  try {
    console.log("event", JSON.stringify(event));
    sqsEventRecords = event.Records;

    const faildSqsItemList = [];

    //we may have multiple sqs events so using for loop
    for (let index = 0; index < sqsEventRecords.length; index++) {
      try {
        //pick sqs record
        const sqsItem = sqsEventRecords[index];

        //pick the dynamo record from sqs event
        const dynamoData = JSON.parse(sqsItem.body);

        //checking if the event from apar table else we ignor the event
        if (dynamoData.dynamoTableName !== SHIPMENT_APAR_TABLE) {
          continue;
        }
        /**
         * added 45 sec delay
         */
        await setDelay(45);

        //converting dynamo obj to js obj
        const shipmentAparData = AWS.DynamoDB.Converter.unmarshall(
          dynamoData.NewImage
        );

        /**
         * if consol no is 0 then its a P2P Non Consol
         * if consol no is greater than 0 then its a consol
         * if shipmentApar.FK_ServiceId is "HS" or "TL" then it's a P2P Consol
         * if shipmentApar.FK_ServiceId is "MT" then it's a Multi Stop Consol
         */
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
