const AWS = require("aws-sdk");
const sns = new AWS.SNS();
const axios = require("axios");
const momentTZ = require("moment-timezone");
const { putItem } = require("../shared/dynamo");
const fs = require('fs');
const FormData = require('form-data');
const { v4: uuidv4 } = require('uuid');
const Flatted = require('flatted');
const sns = new AWS.SNS();

module.exports.handler = async (event, context, callback) => {
    try {
        console.log('event', JSON.stringify(event));
        const records = event.Records;

        const promises = records.map(async (record) => {
            try {
                if (!record.dynamodb.hasOwnProperty('NewImage')) {
                    return;
                }

                const newImage = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
                const { Housebill, shipmentApiRes } = newImage;
                console.log("shipmentApiRes:", shipmentApiRes);
                const parsedShipmentApiRes = JSON.parse(shipmentApiRes);
                const Id = parsedShipmentApiRes.shipmentId;
                console.log("Id:", Id);
                const housebills = Housebill.split(',');
                console.log("housebills:", housebills);

                for (const housebill of housebills) {
                    try {
                        const { b64str, api_status_code, Res } = await callWtRestApi(housebill);
                        const filePath = `/tmp/${housebill}.pdf`;
                        await convertBase64ToPdf(b64str, filePath);
                        const { errorMsg, responseStatus } = await sendPdfToIviaAddDocumentApi(filePath, Id);
                        // const { errorMsg, responseStatus } = await sendPdfToIviaAddDocumentApi(filePath, 1014524);
                        const insertedTimeStamp = momentTZ
                            .tz("America/Chicago")
                            .format("YYYY:MM:DD HH:mm:ss")
                            .toString();
                        const jsonWebsliResponse = Flatted.stringify(Res);
                        const orderNo = await queryTableWithIndex(housebill);
                        console.log("orderNo:", orderNo);
                        const logItem = {
                            Id: uuidv4(),
                            api_status_code: responseStatus,
                            errorMsg: errorMsg,
                            housebill: housebill,
                            inserted_time_stamp: insertedTimeStamp,
                            PK_OrderNo: orderNo,
                            iviaShipmentId: Id,
                            filename: `${housebill}.pdf`,
                            function_name: context.functionName,
                            webSli_request: jsonWebsliResponse,
                            webSli_response_code: api_status_code
                        };
                        console.log("logItem:", logItem);
                        await putItem(process.env.ADD_DOCUMENT_LOGS_TABLE, logItem);
                    } catch (error) {
                        console.error('Error in inner loop', error);
                    }
                }
            } catch (error) {
                console.error('Error in outer loop', error);
            }
        });
        await Promise.all(promises);
        return 'success';
    } catch (error) {
        console.error('Error', error);
        // Send a notification to the SNS topic
        const params = {
            Message: `An error occurred in function ${process.env.FUNCTION_NAME}. Error details: ${error}.`,
            TopicArn: process.env.ERROR_SNS_ARN,
        };
        await sns.publish(params).promise();
        return 'error';
    }
};

// function to call get document websli api
async function callWtRestApi(housebill) {
    try {
        const url = `${process.env.WT_WEBSLI_API_URL}/housebill=${housebill}/doctype=HOUSEBILL`;

        const response = await axios.get(url);
        return {
            b64str: response.data.wtDocs.wtDoc[0].b64str,
            api_status_code: response.status,
            Res: response
        };
    } catch (error) {
        console.error(`Error calling WT REST API for housebill ${housebill}:`, error);
        return 'error';
    }
}

// function to convert base64 string to pdf
function convertBase64ToPdf(base64String, filePath) {
    return new Promise((resolve, reject) => {
        const fileData = Buffer.from(base64String, 'base64');
        fs.writeFile(filePath, fileData, function (err) {
            if (err) {
                console.error('Error converting Base64 to PDF:', err);
                reject(err);
            }
            resolve();
        });
    });
}

// Function to send the PDF to Ivia AddDocument API
async function sendPdfToIviaAddDocumentApi(filePath, Id) {
    const formData = new FormData();
    formData.append('file', fs.createReadStream(filePath));

    const config = {
        method: 'post',
        maxBodyLength: Infinity,
        url: `${process.env.ADD_DOCUMENT_API_URL}/${Id}/documents`,
        headers: {
            'Authorization': `Bearer ${process.env.ADD_DOCUMENT_AUTH_TOKEN}`,
            'Content-Type': 'multipart/form-data',
            ...formData.getHeaders()
        },
        data: formData
    };

    try {
        const response = await axios.request(config);
        console.log('PDF sent to Ivia AddDocument API');
        return {
            success: true,
            errorMsg: "",
            responseStatus: response.status
        };
    } catch (error) {
        console.error('Error sending PDF to Ivia AddDocument API:', error.response.data);
        return {
            success: false,
            errorMsg: error.response.data.errors[0].message,
            responseStatus: error.response.status
        };
    }
}

// function to fetch orderNo for the particular housebill 
async function queryTableWithIndex(housebill) {
    try {
        const params = {
            TableName: process.env.SHIPMENT_HEADER_TABLE,
            IndexName: process.env.SHIPMENT_HEADER_INDEX,
            KeyConditionExpression: 'Housebill = :housebillValue',
            ExpressionAttributeValues: {
                ':housebillValue': housebill
            },
        };

        const dynamodb = new AWS.DynamoDB.DocumentClient();
        const result = await dynamodb.query(params).promise();

        return result.Items[0].PK_OrderNo;
    } catch (error) {
        console.error('Error querying table:', error);
        throw error;
    }
}
