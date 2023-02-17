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
    const unArr = obj.description.split(" ");
    if (unArr[0] === "UN") {
      return unArr.filter((e, i) => {
        return (
          i <= 2 && e.length === 4 && parseInt(e) >= 1 && parseInt(e) <= 3600
        );
      })[0];
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
        refNum1: Joi.string().allow(""), //
        refNum2: Joi.string().allow(""),
      }).required(),
      shipmentDetails: Joi.object({
        stops: Joi.array()
          .items(
            Joi.object({
              stopType: Joi.string().required(), //hardcode P/D
              stopNum: Joi.number().integer().required(), //hardcode if stopType = P then 0 and if stopType = D then 1
              housebills: Joi.array().required(), // shipmentHeader.Housebill (1st we take FK_OrderNo from confirmationCost where FK_SeqNo < 9999 and then we filter the Housebill from shipmentHeader table based on orderNo)
              address: Joi.object({
                address1: Joi.string().allow(""),
                city: Joi.string().allow(""),
                country: Joi.string().required(), // required
                state: Joi.string().allow(""),
                zip: Joi.string().required(), // required
              }).required(),
              cargo: Joi.array().items(
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
              ),
              companyName: Joi.string().allow(""),
              scheduledDate: Joi.number().integer().required(), // shipment header required
              specialInstructions: Joi.string().allow(""),
            }).unknown()
          )
          .required(),
        dockHigh: Joi.string().required(), // req [Y / N] default "N"
        hazardous: Joi.string().required(), // required  shipmentDesc?.Hazmat
        liftGate: Joi.string().required(), // required shipmentApar.ChargeCode
        notes: Joi.string().allow(""),
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

module.exports = {
  prepareBatchFailureObj,
  getLatestObjByTimeStamp,
  getLiftGate,
  getUnNum,
  validatePayload,
  getHazardous,
};
