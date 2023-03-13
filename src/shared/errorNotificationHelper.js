const AWS = require("aws-sdk");

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

async function sendSNSMessage(data) {
  // let error = get(data, "error.message");
  const snsParams = {
    TopicArn: process.env.ERROR_NOTIFICATION_SNS_ARN,
    Subject: ``,
    Message: `Reason for failure: ${error}\n\n${JSON.stringify(data)}`,
  };
  await snsPublishMessage(snsParams);
}

module.exports = { sendSNSMessage };
