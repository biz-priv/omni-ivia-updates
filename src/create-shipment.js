const AWS = require("aws-sdk");
const { v4: uuidv4 } = require("uuid");
const moment = require("moment");
const momentTZ = require("moment-timezone");
const { convert } = require("xmlbuilder2");
const axios = require("axios");
const { putItem } = require("./shared/dynamo");

const IVIA_CREATE_SHIPMENT_URL =
  "https://api-uat.dev.ivia.us/shipments/uncovered";
const IVIA_CREATE_SHIPMENT_TOKEN =
  "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJvY2N1cnJlZF9vbiI6MTY2NzQ3OTcwNDM0MSwidXNlcl9pZCI6MTAwMzk3NSwib3JnX2lkIjoxMDA0NDUxLCJwZXJtaXNzaW9ucyI6W251bGxdLCJzY29wZSI6WyJhcGkiXSwib3Blbl9hcGlfaWQiOiJhNzdjOTZhOS0xNDg2LTQyNzctODA2Zi0zMGIzNDc0NGI5NzEiLCJvcGVuX2FwaV91c2VyX2lkIjoxMDAzOTc2LCJleHAiOjI2Njc0Nzk3MDMsInJlZ2lvbiI6Ik5BIiwianRpIjoiYzA3ODFkMjYtZTZmOS00ZjQ4LWJlOTEtNTNjN2JkNGJhYWIzIiwiY2xpZW50X2lkIjoib3Blbi1hcGkifQ.LZurjH67TW7zX6GvRsmCygtljP4iHlmLMVT4kzO9K5eeZIj6en4hPJiSTIH7-Bd1ytpVLSE_aFwcyQa6Qsx50WOgZJWhvBynwVDTGtLqzXF-rOE_D18i7TS01OueEnGlbnSry2UV9sye1PWy_6s0SEEglZdbhf5WZKs4SU0pcUUfTkTC_Pzjg4RLHRlennS1MY44F0yV5Shococ15bOcICxtfzLE7CYY5wWCv3e1RwVFCRl3_GDsyHl1Jxp9aevX8bepItyn4alFguwgH4gmA2sG3uu4FAdUKXZ4MK85I7SwOMPeWulmMvIgcSAduhvC7ua3xl3Vz3bYmJ_yExXhAA";

const IVIA_XML_API_USER_ID = "eeprod";
const IVIA_XML_API_PASS = "eE081020!";
const IVIA_XML_UPDATE_URL =
  "https://wttest.omnilogistics.com/WTKServices/AirtrakShipment.asmx";
const IVIA_RESPONSE_DDB = "omni-rt-ivia-response-test";

module.exports.handler = async (event, context, callback) => {
  try {
    console.log("event", JSON.stringify(event));
    // const streamRecords = AWS.DynamoDB.Converter.unmarshall(event.Records[0].dynamodb.NewImage);
    const streamRecord = {
      id: "aa03e2c8-7dda-42a5-a508-df749208b4b2",
      data: '{"carrierId":1004451,"refNums":{"refNum1":"5881623","refNum2":"4519683","refNum3":"0"},"shipmentDetails":{"destination":{"address":{"address1":"1234 MOST PLEASANT ST","city":"OTHERPLACE","country":"US","state":"KY","zip":"40475"},"companyName":"TEST CONSIGNEE LLC","scheduledDate":1675319400000,"specialInstructions":""},"dockHigh":"N","hazardous":"N","liftGate":"N","notes":"","origin":{"address":{"address1":"2423 PLEASANT RD","city":"SOMEPLACE","country":"US","state":"GA","zip":"31234"},"cargo":[{"height":"","length":"","packageType":"PIE","quantity":"1","stackable":"N","turnable":"N","weight":"1","width":""}],"companyName":"TEST SHIPPER INC.","scheduledDate":1675132200000,"specialInstructions":""},"unNum":""}}',
      Housebill: "5881623",
      InsertedTimeStamp: "2022:12:05 04:52:54",
    };
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
    return {};
  } catch (error) {
    console.error("Error", error);
    return {};
  }
};

function iviaCreateShipment(payload) {
  return new Promise(async (resolve, reject) => {
    try {
      const config = {
        method: "post",
        url: IVIA_CREATE_SHIPMENT_URL,
        headers: {
          Authorization: IVIA_CREATE_SHIPMENT_TOKEN,
          "Content-Type": "application/json",
        },
        data: JSON.stringify(payload),
      };

      axios(config)
        .then(function (response) {
          resolve({ shipmentId: response.data });
        })
        .catch(function (error) {
          console.log(
            "error:iviaCreateShipment API",
            JSON.stringify(error?.response?.data?.errors ?? "ivia api error")
          );
          reject(error?.response?.data?.errors ?? "ivia api error");
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
          console.log("response", response);
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
          console.log(
            "error",
            JSON.stringify(error?.response?.data ?? "ivia api error")
          );
          reject(error?.response?.data ?? "ivia api error");
        });
    } catch (error) {
      console.log("error:iviaSendUpdate", error);
      reject(error);
    }
  });
}
