const AWS = require('aws-sdk');
const s3 = new AWS.S3();

module.exports.handler = async (event, context, callback) => {
  try {
    console.info('🙂 -> file: ivia-milhouse-movement-updates-processor.js:7 -> module.exports.handler= -> event:', event);
    const s3Bucket = get(event, "Records[0].s3.bucket.name", "");
    const s3Key = get(event, "Records[0].s3.object.key", "");
    // const s3Bucket = 'ivia-milhouse-movement-athena-result-dev';
    // const s3Key = 'pending/02f08175-ec55-4900-8113-7202cb49811c.csv';
    const csvData = await getS3Object(s3Bucket, s3Key);
    console.info('🙂 -> file: ivia-milhouse-movement-updates-processor.js:12 -> module.exports.handler= -> csvData:', csvData);
    const csvToJsonRes = csvStringToJson(csvData);
    console.info('🙂 -> file: ivia-milhouse-movement-updates-processor.js:14 -> module.exports.handler= -> csvToJsonRes:', csvToJsonRes);
    return "success";
  } catch (error) {
    console.error("Error", error);
    return "error";
  }
};

async function getS3Object(bucket, key) {
  try {
    const params = { Bucket: bucket, Key: key };
    const response = await s3.getObject(params).promise();
    return response.Body.toString();
  } catch (error) {
    throw new Error(`S3 error: ${error}`);
  }
}

function csvStringToJson(csvString) {
  const lines = csvString.trim().split('\n');
  const headers = lines[0].split(',');
  const result = [];

  for (let i = 1; i < lines.length; i++) {
    const currentLine = lines[i].split(',');
    const obj = {};

    for (let j = 0; j < headers.length; j++) {
      obj[headers[j]] = currentLine[j];
    }

    result.push(obj);
  }

  return result;
}