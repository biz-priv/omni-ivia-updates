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
    if (["LIFT", "LIFTD", "LIFTP", "TRLPJ"].includes(param.toUpperCase())) {
      console.log("param", param);
      return "Y";
    } else {
      return "N";
    }
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
    const obj = data.length > 0 ? getLatestObjByTimeStamp(data) : {};
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
};
