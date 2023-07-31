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
 * Helper function to get letest record by InsertedTimeStamp
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
 * Fetch shipmentApar data for liftgate based on shipmentDesc.FK_OrderNo list
 * for shipmentDesc data, logic is same as function getHazardous() comments
 *
 * if shipmentApar.chargecode have any of this codes ["LIFT", "LIFTD", "LIFTP", "TRLPJ"] then it returns Y else N
 * @param {*} param
 * @returns Y/N
 */

function getLiftGate(shipmentAparCargo, shipmentHeader) {
  try {
    let val = "N";
    for (let index = 0; index < shipmentHeader.length; index++) {
      const element = shipmentHeader[index];
      if (
        ["LFT PJ BOX", "LFTG BOX"].includes(
          element?.FK_EquipmentCode.toUpperCase()
        )
      ) {
        val = "Y";
      }
    }

    if (val === "Y") {
      return val;
    }

    for (let index = 0; index < shipmentAparCargo.length; index++) {
      const element = shipmentAparCargo[index];
      if (
        ["LIFT", "LIFTD", "LIFTP", "TRLPJ"].includes(
          element?.ChargeCode.toUpperCase()
        )
      ) {
        val = "Y";
      }
    }
    return val;
  } catch (error) {
    return "N";
  }
}

/**
 * p2p non consol:-
 * from tbl_shipmentdesc where fk_orderno=[file number] and ConsolNo=0;
 * fetch shipmentDesc data based on tbl_shipmentapar.fk_orderno
 *
 * p2p consol:-
 * from tbl_shipmentdesc where fk_orderno in (select fk_orderno from tbl_shipmentapar where consolno=[consol number] and consolidation='N');
 * fetch shipmentDesc data based on tbl_shipmentapar.fk_orderno list
 *
 * Multi Stop consol:-
 * from tbl_shipmentdesc where fk_orderno in
 * (select fk_orderno from tbl_consolstopitems where fk_consolstopid in
 *  (select pk_consolstopid from tbl_consolstopheaders as h where h.fk_consolno=[consol number] and h.consolstopnumber=[stop number]));
 *
 * fetch shipmentDesc data based on tbl_consolstopitems.fk_orderno list and
 * fetch tbl_consolstopitems data based on tbl_consolstopheaders.pk_consolstopid list and
 * fetch tbl_consolstopheaders data based on fk_consolno=[consol number] and h.consolstopnumber=[stop number]
 *
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
 * for shipmentDesc data, logic is same as function getHazardous() comments
 *
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
 * JOI validation schema for all 3 scenarios
 * @param {*} payload
 * every field is required only refNum2, specialInstructions may be empty
 */
function validatePayload(payload) {
  const Joi = require("joi");
  try {
    const joySchema = Joi.object({
      carrierId: Joi.number().required(),
      refNums: Joi.object({
        refNum1: Joi.string().required(), // required
        refNum2: Joi.string().allow(""),
        refNum3: Joi.string().allow(""),
      }).required(),
      shipmentDetails: Joi.object({
        stops: Joi.array()
          .items(
            Joi.object({
              stopType: Joi.string().required(),
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
                  height: Joi.number().integer().required(), // required
                  length: Joi.number().integer().required(), // required
                  packageType: Joi.string().required(), // required
                  quantity: Joi.number().integer().required(), // required
                  stackable: Joi.string().required(), // req [Y / N]
                  turnable: Joi.string().required(), // req [Y / N]
                  weight: Joi.number().integer().required(), //req
                  width: Joi.number().integer().required(),
                }).required()
              ),
              companyName: Joi.string().required(), //required
              scheduledDate: Joi.number()
                .integer()
                .positive()
                .min(1)
                .required()
                .messages({
                  "number.base": `"scheduledDate" must be a valid DATE and greater than 2023-01-01`,
                  "number.positive": `"scheduledDate" must be a valid DATE and greater than 2023-01-01`,
                  "number.min": `"scheduledDate" must be a valid DATE and greater than 2023-01-01`,
                  "any.required": `"scheduledDate" must be a valid DATE and greater than 2023-01-01`,
                }), // required
              specialInstructions: Joi.string().allow(""),
              cutoffDate: Joi.number()
                .integer()
                .min(0)
                .allow(null)
                .required()
                .messages({
                  "number.base": `"cutoffDate" must be a valid DATE and greater than 2023-01-01`,
                  "number.min": `"cutoffDate" must be a valid DATE and greater than 2023-01-01`,
                  "any.required": `"cutoffDate" must be a valid DATE and greater than 2023-01-01`,
                })
            }).unknown()
          )
          .min(2)
          .required(),
        dockHigh: Joi.string().required(), // required [Y / N] default "N"
        hazardous: Joi.string().required(), // required  shipmentDesc?.Hazmat
        liftGate: Joi.string().required(), // required shipmentApar.ChargeCode
        unNum: Joi.any().allow("").required(), // accepts only 4 degit number as string
        notes: Joi.string().allow("").required(),
        revenue: Joi.number().required(),
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
 * p2p non consol
 * dateTime:-if tbl_confirmationCost have data
 * then confirmationCost.DeliveryDateTime/confirmationCost.PickupDateTime
 * else take data from tbl_shipmentHeader.ScheduledDateTime/tbl_shipmentHeader.ReadyDateTime
 *
 * zip:-if tbl_confirmationCost have data then confirmationCost.ConZip else take data from shipper/consingee [ConZip]
 * country:-if tbl_confirmationCost have data then confirmationCost.FK_ConCountry else take data from shipper/consingee [FK_ConCountry]
 *
 * p2p consol
 * dateTime:- confirmationCost.DeliveryDateTime / confirmationCost.PickupDateTime
 * zip:- confirmationCost.ConZip
 * country:- confirmationCost.FK_ConCountry
 *
 * Multistop consol
 * dateTime:- conStopHeader.ConsolStopDate + conStopHeader.ConsolStopTimeBegin (for pickup and del both)
 * zip:- conStopHeader.ConZip
 * country:- conStopHeader.ConZip
 *
 * gets offset from getTimeZoneOffsetData() and concats it with the datetime and
 * returns unix timestamp based on zipcode and datetime
 * @param {*} dateTime
 * @param {*} country  if country is US then we split the zip_code string on "-" and take the first element as zip_code
 * @returns
 */
async function getGMTDiff(dateTime, address) {
  const { zip, country } = address;
  try {
    const moment = require("moment");
    if (
      dateTime &&
      dateTime.length > 1 &&
      moment(dateTime).isValid() &&
      dateTime > "2023-01-01"
    ) {
      const zipCode =
        country === "US" && zip.includes("-") ? zip.split("-")[0] : zip;

      const dateArr = dateTime.split(" ");
      let offset = await getTimeZoneOffsetData(dateArr[0], address);
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
 *
 * @param {*} address
 * @param {*} datetime
 * @returns
 */
async function getGOffsetUTC(address, datetime) {
  console.log("***getGOffsetUTC***");
  const axios = require("axios");
  const moment = require("moment");
  const fullAddress = [
    address.address1,
    address.address2,
    address.city,
    address.state,
    address.country,
    address.zip,
  ].join(",");
  try {
    const apiKey = process.env.ADDRESS_MAPPING_G_API_KEY;
    // Get geocode data for address
    const gLatLong = `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(
      fullAddress
    )}&key=${apiKey}`;
    const gLatLongRes = await axios.get(gLatLong);
    if (gLatLongRes.data.status !== "OK") {
      throw new Error(`Unable to geocode`);
    }
    const lat = gLatLongRes.data.results[0].geometry.location.lat;
    const lng = gLatLongRes.data.results[0].geometry.location.lng;
    console.log("gLatLongRes", lat, lng);

    // const timestamp = moment(datetime).diff("1970-01-01", "s");
    const timestamp = moment(datetime).unix();
    console.log("timestamp", timestamp);

    const gOffset = `https://maps.googleapis.com/maps/api/timezone/json?location=${lat}%2C${lng}&timestamp=${timestamp}&key=${apiKey}`;
    console.log("gOffset", gOffset);
    const gOffsetRes = await axios.get(gOffset);
    if (gOffsetRes.data.status !== "OK") {
      throw new Error(`Unable to get timezone`);
    }
    console.log("gOffsetRes", gOffsetRes.data);
    return parseInt(
      (gOffsetRes.data.rawOffset + gOffsetRes.data.dstOffset) / 3600
    );
  } catch (error) {
    console.log("getGOffsetUTC:error", error);
    return "";
  }
}

/**
 * SELECT HoursAway - iif((select top 1 state from tbl_ZipCodes where zip='10012')='AZ',6,iif(datepart(week, '2023-11-05') between 11 and 44,5,6)) FROM tbl_TimeZoneMaster where PK_TimeZoneCode=(SELECT FK_TimeZoneCode FROM tbl_TimeZoneZipCR where zipcode='10012')
 * get the timezone offset for zipcode
 *
 * 1> fetch data from ZIP_CODES table based on zip_code and based on the logic if state is AZ then set offset value = 6
 * else get the week count of the date and check if the weekCount is between 11 - 44 then set offset value = 5 else 6
 *
 * 2>fetch data from TIMEZONE_ZIP_CR based on zip_code take the 1st record and search on
 * TIMEZONE_MASTER table based on TIMEZONE_ZIP_CR.FK_TimeZoneCode
 *
 * 3> based on query logic final value is  TIMEZONE_MASTER.HoursAway column value  - offset value
 * @param {*} params
 * @returns
 */
async function getTimeZoneOffsetData(dateTime, address) {
  const { zip, country } = address;

  try {
    if (country != "US") {
      return await getGOffsetUTC(address, dateTime);
    } else {
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
    }
  } catch (error) {
    console.log("error", error);
    return await getGOffsetUTC(address, dateTime);
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
 * p2p NON consol:- data based on shipmentApar.FK_OrderNo
 * confirmationCost.PickupTimeRange confirmationCost.PickupDateTime
 * if we dont have data on confirmationCost table then fetch from shipmentHeader table
 * shipmentHeader.ReadyDateTimeRange , shipmentHeader.ReadyDateTime
 *
 * p2p consol:-  data based on shipmentApar.FK_OrderNo
 * confirmationCost.PickupTimeRange confirmationCost.PickupDateTime
 *
 * it is not used for multistop consol.
 *
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

/**
 * checkAddressByGoogleApi
 * get full address by google api
 * @param {*} address
 * @returns
 */
async function checkAddressByGoogleApi(mainAddressData) {
  const axios = require("axios");
  let city = "",
    state = "",
    country = "";
  let addressData = mainAddressData;

  try {
    if (
      addressData.address1 != "" &&
      addressData.zip != "" &&
      (addressData.city == "" ||
        addressData.state == "" ||
        addressData.country == "")
    ) {
      console.log("**checkAddressByGoogleApi****");
      const address = [
        addressData.address1,
        addressData.address2,
        addressData.city,
        addressData.state,
        addressData.country,
        addressData.zip,
      ].join(",");

      const apiKey = process.env.ADDRESS_MAPPING_G_API_KEY;
      // Get geocode data for address1
      const geocode1 = await axios.get(
        `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(
          address
        )}&key=${apiKey}`
      );
      console.log("geocode1.data", geocode1.data);
      if (geocode1.data.status !== "OK") {
        throw new Error(`Unable to geocode ${address}`);
      }
      const { results } = geocode1.data;
      console.log("geocode1", JSON.stringify(results[0]));

      for (
        let index = 0;
        index < results[0].address_components.length;
        index++
      ) {
        const element = results[0].address_components[index];
        if (element.types.includes("locality")) {
          city = element.short_name;
        }
        if (element.types.includes("administrative_area_level_1")) {
          state = element.short_name;
        }
        if (element.types.includes("country")) {
          country = element.short_name;
        }
      }
      console.log({ city, state, country });

      return {
        address1: addressData.address1,
        address2: addressData.address2,
        city: addressData.city.length > 0 ? addressData.city : city,
        country: addressData.country.length > 0 ? addressData.country : country,
        state: addressData.state.length > 0 ? addressData.state : state,
        zip: addressData.zip,
      };
    } else {
      return addressData;
    }
  } catch (error) {
    console.log("checkAddressByGoogleApi:error", error);
    return addressData;
  }
}

/**
 * we need to check in the shipmentHeader.OrderDate >= '2023:04:01 00:00:00' - for both nonconsol and consol -> if this condition satisfies, we send the event to Ivia, else we ignore
 * Ignore the event if there is no OrderDate or it is "1900
 */
function checkIfShipmentHeaderOrderDatePass(data) {
  try {
    // if empty then ignor:-
    if (data.length === 0) {
      console.log("shipment-header table have no data, so event Ignored");
      return false;
    }
    let check = true;
    for (let index = 0; index < data.length; index++) {
      const element = data[index];
      if (element?.OrderDate < "2023-04-01 00:00:00") {
        check = false;
      }
    }
    return check;
  } catch (error) {
    console.log("error:checkIfShipmentHeaderOrderDatePass", error);
    return false;
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
  checkAddressByGoogleApi,
  checkIfShipmentHeaderOrderDatePass,
};
