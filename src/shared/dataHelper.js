const AWS = require("aws-sdk");
function prepareBatchFailureObj(data) {
  const batchItemFailures = data.map((e) => ({
    itemIdentifier: e.messageId,
  }));
  console.log("batchItemFailures", batchItemFailures);
  return { batchItemFailures };
}

/**
 * if we got multiple records from one table then we are taking the latest one.
 * @param {*} data
 * @returns
 */
function getLatestObjByTimeStamp(data) {
  if (data.length > 1) {
    return data.sort((a, b) => {
      let atime = a.InsertedTimeStamp.split(" ");
      atime = atime[0].split(":").join("-") + " " + atime[1];

      let btime = b.InsertedTimeStamp.split(" ");
      btime = btime[0].split(":").join("-") + " " + btime[1];

      return new Date(btime) - new Date(atime);
    })[0];
  } else if (data.length === 1) {
    return data[0];
  } else {
    return {};
  }
}

/**
 * get "Y" or "N" based on available lift gate
 * @param {*} param
 * @returns
 */
function getLiftGate(param) {
  try {
    let val = "N";
    for (let index = 0; index < param.length; index++) {
      const element = param[index];
      if (
        ["LIFT", "LIFTD", "LIFTP", "TRLPJ"].includes(
          element?.ChargeCode.toUpperCase()
        )
      ) {
        console.log("param", param);
        val = "Y";
      }
    }
    return val;
  } catch (error) {
    return "N";
  }
}

/**
 * getHazardous
 * @param {*} params
 * @returns
 */
function getHazardous(params) {
  try {
    let val = "N";
    for (let index = 0; index < params.length; index++) {
      const element = params[index];
      if (element?.Hazmat === "Y") {
        val = "Y";
      }
    }
    return val;
  } catch (error) {
    return "N";
  }
}

/**
 * unNum is a number with length 4 and it value should be 0001 to 3600
 * we populate this field if we have "Hazmat" = "Y"
 * example  "UN 2234 ST 1234" so we are taking 2234 as unNum
 * @param {*} param
 * @returns
 */
function getUnNum(param) {
  try {
    const data = param.filter((e) => e.Hazmat.toUpperCase() === "Y");
    const obj = data.length > 0 ? data[0] : {};
    const unArr = obj.Description.split(" ");
    console.log("unArr", unArr);

    if (unArr[0].toUpperCase().includes("UN")) {
      // return unArr.filter((e, i) => {
      //   return (
      //     i <= 2 && e.length === 4 && parseInt(e) >= 1 && parseInt(e) <= 3600
      //   );
      // })[0];
      let unNo = unArr[0];
      unNo = unNo.slice(2, 6);
      if (unNo.length === 4 && parseInt(unNo) <= 4000) {
        return unNo;
      }
    }
    return "";
  } catch (error) {
    return "";
  }
}

/**
 * validate payload
 * @param {*} payload
 */
function validatePayload(payload) {
  const Joi = require("joi");
  try {
    const joySchema = Joi.object({
      carrierId: Joi.number().required(), //hardcode dev:- 1000025
      refNums: Joi.object({
        refNum1: Joi.string().required(), //
        refNum2: Joi.string().allow(""),
      }).required(),
      shipmentDetails: Joi.object({
        stops: Joi.array()
          .items(
            Joi.object({
              stopType: Joi.string().required(), //hardcode P/D
              stopNum: Joi.number().integer().required(),
              housebills: Joi.array().min(1).required(),
              address: Joi.object({
                address1: Joi.string().required(),
                address2: Joi.string().allow(""),
                city: Joi.string().required(),
                country: Joi.string().required(), // required
                state: Joi.string().required(),
                zip: Joi.string().required(), // required
              }).required(),
              cargo: Joi.array().items(
                Joi.object({
                  height: Joi.number().integer().required(),
                  length: Joi.number().integer().required(),
                  packageType: Joi.string().required(), // required
                  quantity: Joi.number().integer().required(), //req
                  stackable: Joi.string().required(), // req [Y / N]
                  turnable: Joi.string().required(), // req [Y / N]
                  weight: Joi.number().integer().required(), //req
                  width: Joi.number().integer().required(),
                }).required()
              ),
              companyName: Joi.string().required(),
              scheduledDate: Joi.number().integer().required(), // shipment header required
              specialInstructions: Joi.string().allow(""),
            }).unknown()
          )
          .min(2)
          .required(),
        dockHigh: Joi.string().required(), // req [Y / N] default "N"
        hazardous: Joi.string().required(), // required  shipmentDesc?.Hazmat
        liftGate: Joi.string().required(), // required shipmentApar.ChargeCode
        unNum: Joi.any().allow("").required(), // accepts only 4 degit number as string
      }).required(),
    }).required();

    const { error, value } = joySchema.validate(payload);
    console.log("", error, value);
    if (error) {
      return error.details.map((e, i) => ({ ["msg" + (i + 1)]: e.message }));
    } else {
      return "";
    }
  } catch (error) {
    console.log("error:validatePayload", error);
    return error;
  }
}

/**
 *
 * @param {*} dateTime
 * @returns
 */
async function getGMTDiff(dateTime, zip, country) {
  try {
    const moment = require("moment");
    if (
      dateTime &&
      dateTime.length > 1 &&
      moment(dateTime).isValid() &&
      dateTime > "1970-01-01"
    ) {
      const zipCode =
        country === "US" && zip.includes("-") ? zip.split("-")[0] : zip;

      const dateArr = dateTime.split(" ");
      let offset = await getTimeZoneOffsetData(dateArr[0], zipCode);
      if (offset <= 0) {
        let number = (offset * -1).toString();
        console.log("number", number);
        number = "-" + (number.length > 1 ? number : "0" + number) + ":00";
        offset = number;
      } else {
        let number = (offset * 1).toString();
        number = "+" + (number.length > 1 ? number : "0" + number) + ":00";
        offset = number;
      }
      const dateStr =
        dateArr[0] +
        "T" +
        (dateArr[1].length > 0 ? dateArr[1] : "00:00:00") +
        offset;
      // return momentTZ(dateStr).tz("Etc/GMT").diff("1970-01-01", "ms");
      const unixDateTime = moment(dateStr).diff("1970-01-01", "ms");
      console.log(
        "dateTime, zipCode",
        dateTime,
        zipCode,
        dateStr,
        unixDateTime
      );
      return unixDateTime;
    } else {
      return "";
    }
  } catch (error) {
    console.log("error", error);
    return "";
  }
}

/**
 * SELECT HoursAway - iif((select top 1 state from tbl_ZipCodes where zip='10012')='AZ',6,iif(datepart(week, '2023-11-05') between 11 and 44,5,6)) FROM tbl_TimeZoneMaster where PK_TimeZoneCode=(SELECT FK_TimeZoneCode FROM tbl_TimeZoneZipCR where zipcode='10012')
 * get the timezone offset for zipcode
 * @param {*} params
 * @returns
 */
async function getTimeZoneOffsetData(dateTime, zip) {
  try {
    const ddb = new AWS.DynamoDB.DocumentClient({
      region: process.env.REGION,
    });

    let offSet = 0;
    /**
     * ZIP_CODES
     * PK:- PK_SeqNo SK:- FK_AirportId
     * index Zip-index  Zip
     */

    const paramZipCode = {
      TableName: process.env.ZIP_CODES,
      IndexName: "Zip-index",
      KeyConditionExpression: "Zip = :Zip",
      ExpressionAttributeValues: {
        ":Zip": zip.toString(),
      },
    };
    let zipCodeData = await ddb.query(paramZipCode).promise();
    zipCodeData = zipCodeData.Items.length > 0 ? zipCodeData.Items[0] : {};
    console.log("zipCodeData", zipCodeData);
    const state = zipCodeData.State;
    if (state === "AZ") {
      offSet = 6;
    } else {
      const cuWeek = getWeekCount(dateTime);
      if (cuWeek >= 11 && cuWeek <= 44) {
        offSet = 5;
      } else {
        offSet = 6;
      }
    }

    /**
     * TIMEZONE_ZIP_CR
     * PK:- ZipCode SK:- FK_TimeZoneCode
     */
    const paramtimezoneCr = {
      TableName: process.env.TIMEZONE_ZIP_CR,
      KeyConditionExpression: "ZipCode = :ZipCode",
      ExpressionAttributeValues: {
        ":ZipCode": zip.toString(),
      },
    };
    let timezoneCrData = await ddb.query(paramtimezoneCr).promise();
    timezoneCrData =
      timezoneCrData.Items.length > 0 ? timezoneCrData.Items[0] : {};
    console.log("timezoneCrData", timezoneCrData);

    /**
     * TIMEZONE_MASTER
     * PK:- PK_TimeZoneCode
     */
    const paramTimezoneMaster = {
      TableName: process.env.TIMEZONE_MASTER,
      KeyConditionExpression: "PK_TimeZoneCode = :PK_TimeZoneCode",
      ExpressionAttributeValues: {
        ":PK_TimeZoneCode": timezoneCrData.FK_TimeZoneCode,
      },
    };
    let timezoneMaster = await ddb.query(paramTimezoneMaster).promise();
    timezoneMaster =
      timezoneMaster.Items.length > 0 ? timezoneMaster.Items[0] : {};
    console.log("timezoneMaster", timezoneMaster);
    offSet = parseInt(timezoneMaster.HoursAway) - offSet;
    console.log("offSet", offSet);
    return offSet;
  } catch (error) {
    return -5;
  }
}

/**
 * creates delay of {sec}
 * @param {*} sec
 * @returns
 */
function setDelay(sec) {
  console.log("delay started");
  return new Promise(async (resolve, reject) => {
    setTimeout(() => {
      console.log("delay end");
      resolve(true);
    }, sec * 1000);
  });
}

function getStatus() {
  return {
    SUCCESS: "SUCCESS",
    FAILED: "FAILED",
    IN_PROGRESS: "IN_PROGRESS",
  };
}

// function getValidDate(date) {
//   try {
//     if (moment(date).isValid() && !date.includes("1970")) {
//       return new Date(date).getTime();
//     } else {
//       return 0;
//     }
//   } catch (error) {
//     return 0;
//   }
// }

// function getNotes(data, type) {
//   try {
//     return data
//       .filter((e) => e.Type.toUpperCase() === type.toUpperCase())
//       .map((e) => e.Note)
//       .join(",");
//   } catch (error) {
//     return "";
//   }
// }

function getWeekCount(date) {
  Date.prototype.getWeek = function (dowOffset) {
    /*getWeek() was developed by Nick Baicoianu at MeanFreePath: http://www.meanfreepath.com */

    dowOffset = typeof dowOffset == "number" ? dowOffset : 0; //default dowOffset to zero
    var newYear = new Date(this.getFullYear(), 0, 1);
    var day = newYear.getDay() - dowOffset; //the day of week the year begins on
    day = day >= 0 ? day : day + 7;
    var daynum =
      Math.floor(
        (this.getTime() -
          newYear.getTime() -
          (this.getTimezoneOffset() - newYear.getTimezoneOffset()) * 60000) /
          86400000
      ) + 1;
    var weeknum;
    //if the year starts before the middle of a week
    if (day < 4) {
      weeknum = Math.floor((daynum + day - 1) / 7) + 1;
      if (weeknum > 52) {
        let nYear = new Date(this.getFullYear() + 1, 0, 1);
        let nday = nYear.getDay() - dowOffset;
        nday = nday >= 0 ? nday : nday + 7;
        /*if the next year starts before the middle of
                  the week, it is week #1 of that year*/
        weeknum = nday < 4 ? 1 : 53;
      }
    } else {
      weeknum = Math.floor((daynum + day - 1) / 7);
    }
    return weeknum;
  };
  // console.log(new Date(date).getWeek());
  return new Date(date).getWeek();
}

function getNotesP2Pconsols(range, datetime, type) {
  const moment = require("moment");
  let msg = "";
  if (type === "p") {
    if (range > datetime) {
      msg =
        "Pickup between " +
        moment(datetime).format("HH:mm") +
        " and " +
        moment(range).format("HH:mm");
    } else {
      msg = "Pickup at " + moment(datetime).format("HH:mm");
    }
  } else {
    if (range > datetime) {
      msg =
        "Deliver between " +
        moment(datetime).format("HH:mm") +
        " and " +
        moment(range).format("HH:mm");
    } else {
      msg = "Deliver at " + moment(datetime).format("HH:mm");
    }
  }
  return msg;
}

/**
 * sort stopes data by stop nums
 * @param {*} data
 * @param {*} key
 * @returns
 */
function sortObjByStopNo(data, key) {
  try {
    return data.sort((a, b) => a[key] - b[key]);
  } catch (error) {
    return data;
  }
}

module.exports = {
  prepareBatchFailureObj,
  getLatestObjByTimeStamp,
  getLiftGate,
  getUnNum,
  validatePayload,
  getHazardous,
  getGMTDiff,
  setDelay,
  getStatus,
  getNotesP2Pconsols,
  sortObjByStopNo,
};
