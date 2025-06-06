/*
 * File: src\v2\create-shipment.js
 * Project: Omni-ivia-updates
 * Author: Bizcloud Experts
 * Date: 2023-09-28
 * Confidential and Proprietary
 */
const AWS = require("aws-sdk");
const { v4: uuidv4 } = require("uuid");
const momentTZ = require("moment-timezone");
const { convert } = require("xmlbuilder2");
const axios = require("axios");
const { putItem, updateItem } = require("../shared/dynamo");
const { validatePayload, getStatus } = require("../shared/dataHelper");
const {
  sendSNSMessage,
  sendSESEmail,
} = require("../shared/errorNotificationHelper");
const _ = require("lodash");

const {
  IVIA_DDB,
  IVIA_CREATE_SHIPMENT_URL,
  IVIA_CREATE_SHIPMENT_TOKEN,
  IVIA_XML_API_USER_ID,
  IVIA_XML_API_PASS,
  IVIA_XML_UPDATE_URL,
  IVIA_RESPONSE_DDB,
  STAGE,
  VENDOR_INVOICE_API_ENDPOINT,
  ADMIN_API_KEY
} = process.env;

// const IVIA_CREATE_SHIPMENT_URL =
//   "https://api-stage.stage.ivia.us/v2/shipments/uncovered";
// const IVIA_CREATE_SHIPMENT_TOKEN =
//   "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJvY2N1cnJlZF9vbiI6MTY3ODkwMTU4MDM5NCwidXNlcl9pZCI6Mzc1NCwib3JnX2lkIjo0MDE3LCJwZXJtaXNzaW9ucyI6W251bGxdLCJzY29wZSI6WyJhcGkiXSwib3Blbl9hcGlfaWQiOiJlYWI0NGM3My1mZTJlLTRmZGUtYWNiZi1jY2YwMWMyMGQzN2YiLCJvcGVuX2FwaV91c2VyX2lkIjozOTIyLCJleHAiOjI2Nzg5MDE1NzksInJlZ2lvbiI6Ik5BIiwianRpIjoiMjY3NDVkNjYtYTY4Ni00NmQwLWEzOGQtYjEwNGZkZDJmZmQzIiwiY2xpZW50X2lkIjoib3Blbi1hcGkifQ.qe1fNGPPrHYhCS6AYU44MiEJ9g78s_pLfbM5tMfai68zLZh64wirM5RkeAUDx1O1oE64pfNTnRIGhbX-zduuZo43aHnfgs4nBie06Pr6vlis7u3W4Vy8IEUwB30iYai9rp42tXkArY03WzRETvyGzQr0igU12v4NyCpEtV7JXIt7q2r5mzqZbjF-9YyAt7Ir4U-xy2-_Cf-lCaswKe2AH1kH_x9e8x_T4vNBKW4uo3T6REqR-me4PanvLkFPXtPQDQr-Sk56aefF9lPmTCum_f0A2Z_PE7dDc0WqNGMImUZcY7-62WD82cWM1eJZ9kpIalw3EqPHbMYphH8VnvOiGw";
module.exports.handler = async (event, context, callback) => {
  try {
    console.log("event", JSON.stringify(event));
    const data = event.Records;
    // processing all the array of records
    for (let index = 0; index < data.length; index++) {
      try {
        if (!data[index].dynamodb.hasOwnProperty("NewImage")) {
          continue;
        }
        //dynamo stream record from omni-ivia table
        const NewImage = data[index].dynamodb.NewImage;

        //converting dynamo obj to normal js obj
        const streamRecord = AWS.DynamoDB.Converter.unmarshall(NewImage);
        const payload = JSON.parse(streamRecord.data);

        //all the other status are ignored, only IN_PROGRESS go for ivia
        if (streamRecord.status !== getStatus().IN_PROGRESS) {
          continue;
        }

        //ivia main api
        const iviaCSRes = await iviaCreateShipment(payload);
        console.log("iviaCSRes", iviaCSRes);
        let iviaXmlUpdateRes = {};
        let iviaXmlUpdateResArr = [];

        if (
          iviaCSRes &&
          iviaCSRes?.shipmentId &&
          iviaCSRes.shipmentId.toString().length > 0 &&
          STAGE.toUpperCase() != "STG"
        ) {
          const houseBills = streamRecord.Housebill.split(",");
          console.log("houseBills", houseBills);

          //sending update to WorldTrack for all housebill for the shipment id
          for (let index = 0; index < houseBills.length; index++) {
            const element = houseBills[index];

            //WT upadte xml api
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

        //preparing dynamo obj for ddb:- omni-ivia-response
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
          errorMsg:
            iviaCSRes.status === getStatus().FAILED
              ? JSON.stringify(iviaCSRes.error)
              : "",
          errorReason:
            iviaCSRes.status === getStatus().FAILED ? "IVIA API ERROR" : "",
        };
        console.log("resPayload", resPayload);

        //add Ivia and WT responses to dynamo db
        await putItem(IVIA_RESPONSE_DDB, resPayload);

        /**
         * preparing update payload for ddb:- Omni-ivia
         * we update the success/fail respone and error msg
         */
        const updatePayload = {
          ...streamRecord,
          status: iviaCSRes.status,
          errorMsg:
            iviaCSRes.status === getStatus().FAILED
              ? JSON.stringify(iviaCSRes.error)
              : "",
          errorReason:
            iviaCSRes.status === getStatus().FAILED ? "IVIA API ERROR" : "",
        };
        await updateItem(IVIA_DDB, { id: streamRecord.id }, updatePayload);

        //send error msg if failed to create shipment to IVIA
        if (iviaCSRes.status === getStatus().FAILED) {
          await sendSNSMessage(updatePayload);
        }
        const housebills = streamRecord.Housebill.split(",");
        console.log("houseBills", housebills);

        await Promise.all(
          housebills.map(async (housebill) => {
            try {
              // Process to update the vendor reference in housebill level
              await updateVendorReference({
                housebill,
                shipmentId: iviaCSRes.shipmentId,
                houseBillString: _.get(streamRecord, "Housebill", ""),
                orderId: _.get(streamRecord, "FK_OrderNo", ""),
                consolNo: _.get(streamRecord, "ConsolNo", ""),
              });
            } catch (error) {
              console.error(
                `Error updating vendor reference for housebill ${housebill}:`,
                error.message
              );
              await sendSESEmail({
                message: `
              <!DOCTYPE html>
              <html>
              <head>
                <style>
                  body {
                    font-family: Arial, sans-serif;
                  }
                  .container {
                    padding: 20px;
                    border: 1px solid #ddd;
                    border-radius: 5px;
                    background-color: #f9f9f9;
                  }
                  .highlight {
                    font-weight: bold;
                  }
                </style>
              </head>
              <body>
                <div class="container">
                  <p>Dear Team,</p>
                  <p>There was an error updating the Vendor Reference for housebill: <strong>${housebill}</strong>.</p>
                  <p><strong>Error Details:</strong>${formatErrorMessage(error.message)}</p>
                  <p>Thank you,<br>
                  Omni Automation System</p>
                  <p style="font-size: 0.9em; color: #888;">Note: This is a system generated email, Please do not reply to this email.</p>
                </div>
              </body>
              </html>
              `,
                subject: `${STAGE.toUpperCase()} - Vendor Reference Update Failed (Housebill: ${housebill}, IVIA Shipment Number: ${
                  iviaCSRes.shipmentId
                })`,
              });
            }
          })
        );

        //  Process to update the vendor reference in consolidation level for P2PConsole and MultistopConsole shipments.
        if (
          _.get(streamRecord, "payloadType", "") !== "P2PNonConsol" &&
          _.get(streamRecord, "ConsolNo", "") !== "0"
        ) {
          try {
            await updateVendorReference({
              shipmentId: _.get(iviaCSRes, "shipmentId", ""),
              houseBillString: _.get(streamRecord, "Housebill", ""),
              orderId: _.get(streamRecord, "FK_OrderNo", ""),
              consolNo: _.get(streamRecord, "ConsolNo", ""),
            });
          } catch (error) {
            console.error(
              "🚀 ~ file: create-shipment.js:175 ~ module.exports.handler= ~ error:",
              error
            );
            await sendSESEmail({
              message: `
              <!DOCTYPE html>
              <html>
              <head>
                <style>
                  body {
                    font-family: Arial, sans-serif;
                  }
                  .container {
                    padding: 20px;
                    border: 1px solid #ddd;
                    border-radius: 5px;
                    background-color: #f9f9f9;
                  }
                  .highlight {
                    font-weight: bold;
                  }
                </style>
              </head>
              <body>
                <div class="container">
                  <p>Dear Team,</p>
                  <p>There was an error updating the Vendor Reference for Consolidation: <strong>${_.get(
                    streamRecord,
                    "ConsolNo",
                    ""
                  )}</strong>.</p>
                 <p><strong>Error Details:</strong>${formatErrorMessage(error.message)}</p>
                  <p>Thank you,<br>
                  Omni Automation System</p>
                  <p style="font-size: 0.9em; color: #888;">Note: This is a system generated email, Please do not reply to this email.</p>
                </div>
              </body>
              </html>
              `,
              subject: `${STAGE.toUpperCase()} - Vendor Reference Update Failed (Consolidation: ${_.get(
                streamRecord,
                "ConsolNo",
                ""
              )}, IVIA Shipment Number: ${iviaCSRes.shipmentId})`,
            });
          }
        }
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

function formatErrorMessage(message) {
  return message.replace(/\n/g, '<br>');
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
      console.log(
        "Request to ivia",
        momentTZ.tz("America/Chicago").format("YYYY:MM:DD HH:mm:ss:SSS")
      );

      axios(config)
        .then(function (response) {
          console.log(
            "Response recieved from ivia",
            momentTZ.tz("America/Chicago").format("YYYY:MM:DD HH:mm:ss:SSS")
          );
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
 * update WorlTrack XML api
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

async function updateVendorReference({
  orderId,
  consolNo,
  houseBillString,
  housebill,
  shipmentId,
}) {
  const data = housebill
    ? {
        vendorInvoiceRequest: {
          housebill,
          operation: "UPDATE",
          vendorReference: shipmentId.toString(),
          vendorId: "T19262",
        },
      }
    : {
        vendorInvoiceRequest: {
          fileNumber: consolNo,
          operation: "UPDATE",
          vendorReference: shipmentId.toString(),
          vendorId: "T19262",
        },
      };

  const config = {
    method: "post",
    maxBodyLength: Infinity,
    url: VENDOR_INVOICE_API_ENDPOINT,
    headers: {
      "Content-Type": "application/json",
      "x-api-key": ADMIN_API_KEY,
    },
    data: JSON.stringify(data),
  };

  try {
    const response = await axios.request(config);
    return _.get(response, "data");
  } catch (error) {
    const errorMessage = JSON.stringify(
      _.get(error, "response.data", error.message)
    );

    throw new Error(`
      \nError in Update Vendor Reference API.
      \nFK_OrderNo: ${orderId}
      \nConsolNo: ${consolNo}
      \nHousebill: ${houseBillString}
      \nError Details: ${errorMessage}
      \nPayload:
      \n${JSON.stringify(data)}
      \nNote: The Shipment is tendered to IVIA with Shipment Number: ${shipmentId} but failed to update the vendor reference.
    `);
  }
}
