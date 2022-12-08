const AWS = require("aws-sdk");
const { v4: uuidv4 } = require("uuid");
const momentTZ = require("moment-timezone");
const { convert } = require("xmlbuilder2");
const axios = require("axios");
const { putItem } = require("./shared/dynamo");

const IVIA_CREATE_SHIPMENT_URL = process.env.IVIA_CREATE_SHIPMENT_URL;
const IVIA_CREATE_SHIPMENT_TOKEN = process.env.IVIA_CREATE_SHIPMENT_TOKEN;
const IVIA_XML_API_USER_ID = process.env.IVIA_XML_API_USER_ID;
const IVIA_XML_API_PASS = process.env.IVIA_XML_API_PASS;
const IVIA_XML_UPDATE_URL = process.env.IVIA_XML_UPDATE_URL;
const IVIA_RESPONSE_DDB = process.env.IVIA_RESPONSE_DDB;

module.exports.handler = async (event, context, callback) => {
  try {
    console.log("event", JSON.stringify(event));
    const data = event.Records;
    for (let index = 0; index < data.length; index++) {
      const NewImage = data[index].dynamodb.NewImage;
      const streamRecord = AWS.DynamoDB.Converter.unmarshall(NewImage);
      const payload = JSON.parse(streamRecord.data);
      console.log("payload", payload);

      const iviaCSRes = await iviaCreateShipment(payload);
      console.log("iviaCSRes", iviaCSRes);

      const iviaXmlUpdateRes = await iviaSendUpdate(
        streamRecord.Housebill,
        iviaCSRes.shipmentId
      );
      console.log("iviaXmlUpdateRes", JSON.stringify(iviaXmlUpdateRes));

      const resPayload = {
        id: uuidv4(),
        payload: streamRecord.data,
        Housebill: streamRecord.Housebill,
        shipmentApiRes: JSON.stringify(iviaCSRes),
        xmlUpdateRes: JSON.stringify(iviaXmlUpdateRes),
        InsertedTimeStamp: momentTZ
          .tz("America/Chicago")
          .format("YYYY:MM:DD HH:mm:ss")
          .toString(),
      };
      console.log("resPayload", resPayload);
      await putItem(IVIA_RESPONSE_DDB, resPayload);
    }
    return "success";
  } catch (error) {
    console.error("Error", error);
    return "error";
  }
};

function iviaCreateShipment(payload) {
  return new Promise(async (resolve, reject) => {
    try {
      const config = {
        method: "post",
        url: IVIA_CREATE_SHIPMENT_URL,
        headers: {
          Authorization: "Bearer " + IVIA_CREATE_SHIPMENT_TOKEN,
          "Content-Type": "application/json",
        },
        data: JSON.stringify(payload),
      };

      axios(config)
        .then(function (response) {
          resolve({ shipmentId: response.data });
        })
        .catch(function (error) {
          console.log("error:iviaCreateShipment API", error?.response);
          resolve(error?.response?.data ?? "ivia api error");
        });
    } catch (error) {
      console.log("error:iviaCreateShipment", error);
      reject(error);
    }
  });
}

async function iviaSendUpdate(houseBill, shipmentId) {
  return new Promise(async (resolve, reject) => {
    try {
      const data = `<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"> <soap:Header> <AuthHeader xmlns="http://tempuri.org/"><UserName>${IVIA_XML_API_USER_ID}</UserName><Password>${IVIA_XML_API_PASS}</Password></AuthHeader></soap:Header><soap:Body><WriteTrackingNote xmlns="http://tempuri.org/"><HandlingStation></HandlingStation><HouseBill>${houseBill}</HouseBill><TrackingNotes><TrackingNotes><TrackingNoteMessage>Ivia shipment number ${shipmentId}</TrackingNoteMessage></TrackingNotes></TrackingNotes></WriteTrackingNote></soap:Body></soap:Envelope>`;
      const config = {
        method: "post",
        url: IVIA_XML_UPDATE_URL,
        headers: {
          "Content-Type": "text/xml",
          Accept: "text/xml",
        },
        data: data,
      };

      axios(config)
        .then(function (response) {
          console.log("response", response.data);
          const obj = convert(response.data, { format: "object" });
          if (
            obj["soap:Envelope"]["soap:Body"].WriteTrackingNoteResponse
              .WriteTrackingNoteResult === "Success"
          ) {
            resolve(obj);
          } else {
            obj["soap:Envelope"][
              "soap:Body"
            ].WriteTrackingNoteResponse.WriteTrackingNoteResult = "Failed";
            resolve(obj);
          }
        })
        .catch(function (error) {
          console.log("error", error?.response);
          resolve(error?.response?.data ?? "ivia SendUpdate error");
        });
    } catch (error) {
      console.log("error:iviaSendUpdate", error);
      reject(error);
    }
  });
}
