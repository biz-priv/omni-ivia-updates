const AWS = require("aws-sdk");

/**
 * if any sqs event is failed to process for some reason then we again sending it back to sqs,
 * which will retry again
 * @param {*} data
 * @returns
 */
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
 * if chargecode have any of this codes ["LIFT", "LIFTD", "LIFTP", "TRLPJ"] then it returns Y else N
 * @param {*} param
 * @returns Y/N
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
 * if Hazmat = Y from any of the records then it returns Y else N
 * @param {*} params
 * @returns Y/N
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
 * example  "UN2234 ST 1234" so we are taking 2234 as unNum
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
 * JOI validation schema
 * @param {*} payload
 * every field is required only refNum2, specialInstructions may be empty
 */
function validatePayload(payload) {
  const Joi = require("joi");
  try {
    const joySchema = Joi.object({
      carrierId: Joi.number().required(), //hardcode dev:- 1000025
      refNums: Joi.object({
        refNum1: Joi.string().required(), // required
        refNum2: Joi.string().allow(""),
      }).required(),
      shipmentDetails: Joi.object({
        stops: Joi.array()
          .items(
            Joi.object({
              stopType: Joi.string().required(), //hardcode P/D
              stopNum: Joi.number().integer().required(), // required
              housebills: Joi.array().min(1).required(), // required
              address: Joi.object({
                address1: Joi.string().required(), // required
                address2: Joi.string().allow(""), // required
                city: Joi.string().required(), // required
                country: Joi.string().required(), // required
                state: Joi.string().required(), // required
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
              companyName: Joi.string().required(), //required
              scheduledDate: Joi.number().integer().required(), // required
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
 * returns unix timestamp based on zipcode and datetime
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

/**
 * status list for the dynamoDb tables
 * omni-ivia and omni-ivia-response
 * @returns
 */
function getStatus() {
  return {
    SUCCESS: "SUCCESS",
    FAILED: "FAILED",
    IN_PROGRESS: "IN_PROGRESS",
  };
}

/**
 * helper function to get the current week count based on date
 * @param {*} date
 * @returns
 */
function getWeekCount(date) {
  Date.prototype.getWeek = function (dowOffset) {
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
  return new Date(date).getWeek();
}

/**
 * prepare notes based on below variables
 * @param {*} range datetime value
 * @param {*} datetime datetime value
 * @param {*} type p or d (p= pickup, d = delivery) type stops
 * @returns
 */
function getNotesP2Pconsols(range, datetime, type) {
  try {
    const moment = require("moment");
    const pickupOrDelRangeTime = range.split(" ")[1];
    const pickupOrDelDateTime = datetime.split(" ")[1];
    let msg = "";
    //pickup type logic
    if (type === "p") {
      if (pickupOrDelRangeTime > pickupOrDelDateTime) {
        msg =
          "Pickup between " +
          moment(datetime).format("HH:mm") +
          " and " +
          moment(range).format("HH:mm");
      } else {
        msg = "Pickup at " + moment(datetime).format("HH:mm");
      }
    } else {
      //delivery type logic
      if (pickupOrDelRangeTime > pickupOrDelDateTime) {
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
  } catch (error) {
    console.log("getNotesP2Pconsols:error", error);
    return "";
  }
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
