const AWS = require("aws-sdk");
const { v4: uuidv4 } = require("uuid");
const momentTZ = require("moment-timezone");
const { convert } = require("xmlbuilder2");
const axios = require("axios");
const { putItem, updateItem } = require("../shared/dynamo");
const { validatePayload, getStatus } = require("../shared/dataHelper");

const {
  IVIA_DDB,
  // IVIA_CREATE_SHIPMENT_URL,
  // IVIA_CREATE_SHIPMENT_TOKEN,
  IVIA_XML_API_USER_ID,
  IVIA_XML_API_PASS,
  IVIA_XML_UPDATE_URL,
  IVIA_RESPONSE_DDB,
} = process.env;

const IVIA_CREATE_SHIPMENT_URL =
  "https://api-stage.stage.ivia.us/v2/shipments/uncovered";
const IVIA_CREATE_SHIPMENT_TOKEN =
  "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJvY2N1cnJlZF9vbiI6MTY2OTc0MDg5OTk3OSwidXNlcl9pZCI6MzcxMCwib3JnX2lkIjo0MDA5LCJwZXJtaXNzaW9ucyI6W251bGxdLCJzY29wZSI6WyJhcGkiXSwib3Blbl9hcGlfaWQiOiI5NjFlOWY1ZC0xMjA4LTQ5MDUtYmUwMS1hMGIxNTQ5ZDZhYzAiLCJvcGVuX2FwaV91c2VyX2lkIjozNzExLCJleHAiOjI2Njk3NDA4OTgsInJlZ2lvbiI6Ik5BIiwianRpIjoiMTFhOWYzNTQtMjgzNy00OGFkLWJkNjItYzY5ZDk0YTA4YTEyIiwiY2xpZW50X2lkIjoib3Blbi1hcGkifQ.HO3kQ4EKSaDbgPigdytn2s7JndR_uYSDYKC3gZWZDe5e0eKKQWj1K18q9shyD3CXgQn59TwnfNl67F9BwPS5UZ1qgpU6f5MwY2B41QrruJUUrzIJJB1L1-ZDuQJqf05VsVkG5tmBC8oSNZgym9HCCtS0N2YodcC3VhOZreNdyjCHZMX4UsBjc66tWEpoD2y-DjSM_2JVs1fRDjnuUjla_N5Iydez0Pcgl7ZsD20QFlUKK4fqGVOafwLw1eApORmVdfUl11Or2QR5_ovH9plHEud1sb-l7CAa0rKB6f1losIJasxbG2fc3OG0ArSJHxTeZz7_clkxrfRFxFGEaj7z6Q";

module.exports.handler = async (event, context, callback) => {
  try {
    console.log("event", JSON.stringify(event));
    const data = event.Records;
    // processing all the array of records
    for (let index = 0; index < data.length; index++) {
      try {
        const NewImage = data[index].dynamodb.NewImage;
        const streamRecord = AWS.DynamoDB.Converter.unmarshall(NewImage);
        const payload = JSON.parse(streamRecord.data);
        if (streamRecord.status === getStatus().FAILED) {
          continue;
        }
        //validate the payload
        validatePayload(payload);

        //ivia main api
        const iviaCSRes = await iviaCreateShipment(payload);
        console.log("iviaCSRes", iviaCSRes);
        let iviaXmlUpdateRes = {};
        let iviaXmlUpdateResArr = [];

        if (
          iviaCSRes &&
          iviaCSRes?.shipmentId &&
          iviaCSRes.shipmentId.toString().length > 0
        ) {
          const houseBills = streamRecord.Housebill.split(",");
          console.log("houseBills", houseBills);
          for (let index = 0; index < houseBills.length; index++) {
            const element = houseBills[index];

            //ivia upadte xml api
            iviaXmlUpdateRes = await iviaSendUpdate(
              element,
              iviaCSRes.shipmentId
            );
            console.log("iviaXmlUpdateRes", JSON.stringify(iviaXmlUpdateRes));
            iviaXmlUpdateResArr = [
              ...iviaXmlUpdateResArr,
              {
                shipmentId: iviaCSRes.shipmentId,
                houseBillNo: element,
                ...iviaXmlUpdateRes,
              },
            ];
          }
        }

        const resPayload = {
          id: uuidv4(),
          payload: streamRecord.data,
          Housebill: streamRecord.Housebill,
          ConsolNo: streamRecord?.ConsolNo ?? "",
          FK_OrderNo: streamRecord?.FK_OrderNo ?? "",
          payloadType: streamRecord?.payloadType ?? "",
          shipmentApiRes: JSON.stringify(iviaCSRes),
          xmlUpdateRes: JSON.stringify(iviaXmlUpdateResArr),
          InsertedTimeStamp: momentTZ
            .tz("America/Chicago")
            .format("YYYY:MM:DD HH:mm:ss")
            .toString(),
          status: iviaCSRes.status,
        };
        console.log("resPayload", resPayload);

        //update all the response to dynamo db
        await putItem(IVIA_RESPONSE_DDB, resPayload);

        await updateItem(
          IVIA_DDB,
          { id: streamRecord.id },
          { ...streamRecord, status: iviaCSRes.status }
        );
      } catch (error) {
        console.error("Error:in For loop", error);
      }
    }
    return "success";
  } catch (error) {
    console.error("Error", error);
    return "error";
  }
};

/**
 * create shipment api
 * @param {*} payload
 * @returns
 */
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
          resolve({ shipmentId: response.data, status: getStatus().SUCCESS });
        })
        .catch(function (error) {
          console.log("error:iviaCreateShipment API", error?.response);
          resolve({
            status: getStatus().FAILED,
            error: error?.response?.data ?? "ivia api error",
          });
        });
    } catch (error) {
      console.log("error:iviaCreateShipment", error);
      reject({ status: getStatus().FAILED, error });
    }
  });
}

/**
 * update xm api
 * @param {*} houseBill
 * @param {*} shipmentId
 * @returns
 */
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
