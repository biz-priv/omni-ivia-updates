const AWS = require('aws-sdk');
const athena = new AWS.Athena();

const ATHENA_RESULT_S3_BUCKET = 'ivia-milhouse-movement-athena-result-dev/pending';

module.exports.handler = async (event, context, callback) => {
  try {
    const query = `with w1 as (
      select
          id,
          max(transact_id) as transact_id
      from
          orders
      group by
          id),
      w2 as (
      select
          max(transact_id) as transact_id,
          id
      from
          stop
      group by
          id),
      w3 as (
      select
          max(transact_id) as transact_id,
          id
      from
          movement_order
      group by
          id ),
      w4 as (
      select
          max(transact_id) as transact_id,
          id
      from
          movement
      group by
          id )
      select m.id as movement_id,
          m.override_payee_id as carrier_id,
          m.status as movement_status,
          o.hazmat as hazardous,
          o.blnum,
          s1.stop_type, s1.order_sequence,s1.address, s1.address2, s1.city_name,s1.zip_code, s1.state, o.company_id,
          o.weight, o.pieces
      from movement m
      join w4 on m.id = w4.id and m.transact_id = w4.transact_id
      left join (select s.* from stop s join w2 on s.id = w2.id and s.transact_id = w2.transact_id) s1 on s1.movement_id = m.id
      left join (select mo.order_id as order_id, mo.movement_id as movement_id from movement_order mo join w3 on mo.id = w3.id and mo.transact_id = w3.transact_id) mo on mo.movement_id = m.id
      left join (select o.* from orders o join w1 on o.id = w1.id and o.transact_id = w1.transact_id) o on o.id = mo.order_id
      where m.override_payee_id = 'MILLFLNC'
          and m.status = 'C';`
    const result = await getDataFromAthena(query)
    console.info('🙂 -> file: milhouse.js:40 -> result:', result);
    return "success";
  } catch (error) {
    console.error("Error", error);
    return "error";
  }
};

async function getDataFromAthena(query) {

  const params = {
    QueryString: query,
    QueryExecutionContext: {
      Database: 'dw-etl-lvlp-prod',
      Catalog: 'AwsDataCatalog'
    },
    ResultConfiguration: {
      OutputLocation: `s3://${ATHENA_RESULT_S3_BUCKET}/`
    }
  };

  try {
    const queryExecutionResult = await athena.startQueryExecution(params).promise();
    const queryExecutionId = queryExecutionResult.QueryExecutionId;
    console.log('Query execution ID:', queryExecutionId);
    console.log("delay started")
    // await setTimeoutPromise(30000);
    console.log("delay completed")

    // const bucketName = ATHENA_RESULT_S3_BUCKET;
    // const s3params = {
    //   Bucket: bucketName,
    //   Key: queryExecutionId + '.csv'
    // };
    // const data = await s3.getObject(s3params).promise();
    // const csvContent = data.Body.toString('utf-8');
    // const jsonArray = await csvtojson().fromString(csvContent);
    // console.log('File content:', csvContent);
    // console.log(jsonArray)
    // console.log(orderArray.length)
    return queryExecutionId
  } catch (error) {
    console.error('Error:', error);
    console.error('Error:', error.AthenaErrorCode);
    console.error('Error:', error.Message);
  }
}