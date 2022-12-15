const AWS = require("aws-sdk");
const Joi = require("joi");
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
    // processing all the array of records
    for (let index = 0; index < data.length; index++) {
      try {
        const NewImage = data[index].dynamodb.NewImage;
        const streamRecord = AWS.DynamoDB.Converter.unmarshall(NewImage);
        const payload = JSON.parse(streamRecord.data);

        //validate the payload
        validatePayload(payload);

        //ivia main api
        const iviaCSRes = await iviaCreateShipment(payload);
        console.log("iviaCSRes", iviaCSRes);
        let iviaXmlUpdateRes = {};
        if (
          iviaCSRes &&
          iviaCSRes?.shipmentId &&
          iviaCSRes.shipmentId.length > 0
        ) {
          //ivia upadte xml api
          iviaXmlUpdateRes = await iviaSendUpdate(
            streamRecord.Housebill,
            iviaCSRes.shipmentId
          );
          console.log("iviaXmlUpdateRes", JSON.stringify(iviaXmlUpdateRes));
        }

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

        //update all the response to dynamo db
        await putItem(IVIA_RESPONSE_DDB, resPayload);
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
 * validate payload
 * @param {*} payload
 */
function validatePayload(payload) {
  try {
    const joySchema = Joi.object({
      carrierId: Joi.number().required(), //hardcode
      refNums: Joi.object({
        refNum1: Joi.string().allow(""),
        refNum2: Joi.string().allow(""),
        refNum3: Joi.string().allow(""),
      }).required(),
      shipmentDetails: Joi.object({
        destination: Joi.object({
          address: Joi.object({
            address1: Joi.string().allow(""),
            city: Joi.string().allow(""),
            country: Joi.string().required(), // required
            state: Joi.string().required(), // required
            zip: Joi.string().required(), // required
          }).required(),
          companyName: Joi.string().allow(""),
          scheduledDate: Joi.number().integer().required(), // shipment header required
          specialInstructions: Joi.string().allow(""),
        }).required(),
        dockHigh: Joi.string().required(), // req [Y / N]
        hazardous: Joi.string().required(), // required
        liftGate: Joi.string().required(), // required
        notes: Joi.string().allow(""),
        origin: Joi.object({
          address: Joi.object({
            address1: Joi.string().allow(""),
            city: Joi.string().allow(""),
            country: Joi.string().required(), // required
            state: Joi.string().allow(""),
            zip: Joi.string().required(), // required
          }).required(),
          cargo: Joi.array()
            .items(
              Joi.object({
                height: Joi.number().integer().allow(""),
                length: Joi.number().integer().allow(""),
                packageType: Joi.string().required(), // required
                quantity: Joi.number().integer().required(), //req
                stackable: Joi.string().required(), // req [Y / N]
                turnable: Joi.string().required(), // req [Y / N]
                weight: Joi.number().integer().required(), //req
                width: Joi.number().integer().allow(""),
              }).required()
            )
            .required(),
          companyName: Joi.string().allow(""),
          scheduledDate: Joi.number().integer().required(), // shipment header required
          specialInstructions: Joi.string().allow(""),
        }).required(),
        unNum: Joi.any().allow("").required(), // accepts only 4 degit number as string
      }).required(),
    }).required();
    const { error, value } = joySchema.validate(payload);
    if (error) {
      throw error;
    }
  } catch (error) {
    console.log("error:validatePayload", error);
    throw error;
  }
}

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
