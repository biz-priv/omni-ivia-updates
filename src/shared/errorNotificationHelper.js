/*
* File: src\shared\errorNotificationHelper.js
* Project: Omni-ivia-updates
* Author: Bizcloud Experts
* Date: 2023-03-15
* Confidential and Proprietary
*/
const AWS = require("aws-sdk");
/**
 * sns publish function
 * @param {*} params
 */
async function snsPublishMessage(params) {
  try {
    const sns = new AWS.SNS({ apiVersion: "2010-03-31" });
    await sns.publish(params).promise();
  } catch (e) {
    console.error(
      "Sns publish message error: ",
      e,
      "\nparams: ",
      JSON.stringify(params)
    );
    throw "SnsPublishMessageError";
  }
}

/**
 * sns notification function
 * it prepare the notification msg
 * @param {*} data
 */
async function sendSNSMessage(data) {
  try {
    const snsParams = {
      TopicArn: process.env.ERROR_NOTIFICATION_SNS_ARN,
      Subject: `IVIA ERROR NOTIFICATION - ${process.env.STAGE}`,
      Message: `Reason for failure: \n 
                ErrorMSG:- ${data.errorMsg} \n 
                errorReason:- ${data.errorReason} \n 
                ConsolNo:- ${data.ConsolNo} \n 
                FK_OrderNo:- ${data.FK_OrderNo} \n 
                payloadType:- ${data.payloadType} \n\n 
                IVIA Payload:- ${data.data} \n\n 
                DB OBJ:- ${JSON.stringify(data)}
                `,
    };
    await snsPublishMessage(snsParams);
  } catch (error) {
    console.log("error:sendSNSMessage", error);
  }
}

module.exports = { sendSNSMessage };
