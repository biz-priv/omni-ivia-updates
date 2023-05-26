const AWS = require("aws-sdk");
const axios = require("axios");
const { putItem, updateItem } = require("../shared/dynamo");
const { validatePayload, getStatus } = require("../shared/dataHelper");
const fs = require('fs');
const FormData = require('form-data');



exports.handler = async (event, context, callback) => {
    try {
        console.log('event', JSON.stringify(event));
        const records = event.Records;

        records.forEach(async (record) => {
            try {
                if (!record.dynamodb.hasOwnProperty('NewImage')) {
                    return;
                }

                const newImage = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
                const { FK_OrderNo, Housebill, shipmentApiRes } = newImage;
                console.log("shipmentApiRes:", shipmentApiRes);
                const parsedShipmentApiRes = JSON.parse(shipmentApiRes);
                const Id = parsedShipmentApiRes.shipmentId;
                console.log("Id:", Id);
                const housebills = Housebill.split(',');
                console.log("housebills:", housebills);

                for (const housebill of housebills) {
                    const base64String = await callWtRestApi(housebill);
                    const filePath = `"/tmp/"${housebill}.pdf`;
                    await convertBase64ToPdf(base64String, filePath);
                    await sendPdfToIviaBolApi(filePath, 1014524);
                }
            } catch (error) {
                console.error('Error in forEach loop', error);
            }
        });
        return 'success';
    } catch (error) {
        console.error('Error', error);
        return 'error';
    }
};

async function callWtRestApi(housebill) {
    try {
        const url = `${process.env.GET_DOCUMENT_API}/housebill=${housebill}/doctype=HOUSEBILL`;

        const response = await axios.get(url);
        return response.data.wtDocs.wtDoc[0].b64str;
    } catch (error) {
        console.error(`Error calling WT REST API for housebill ${housebill}:`, error);
        throw error;
    }
}

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

// Function to send the PDF to Ivia BOL API
function sendPdfToIviaBolApi(filePath, Id) {
    return new Promise((resolve, reject) => {
        const formData = new FormData();
        formData.append('file', fs.createReadStream(filePath));

        const config = {
            method: 'post',
            maxBodyLength: Infinity,
            url: `https://uat-api.dev.ivia.us/shipments/${Id}/documents`,
            headers: {
                'Authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJvY2N1cnJlZF9vbiI6MTY3OTA0Njk1MjU1NSwidXNlcl9pZCI6MTAwMzk3NSwib3JnX2lkIjoxMDA0NDUxLCJwZXJtaXNzaW9ucyI6W251bGxdLCJzY29wZSI6WyJhcGkiXSwib3Blbl9hcGlfaWQiOiJkNDM2NGRhYy05ZGRhLTRmZDUtYWYyYS04ZGNmYTVjMjA5MzUiLCJvcGVuX2FwaV91c2VyX2lkIjoxMDA0MzY1LCJleHAiOjI2NzkwNDY5NTEsInJlZ2lvbiI6Ik5BIiwianRpIjoiZGVmMzUwYWMtM2IxMS00ZmFlLThlYTEtNGJiYjkxMGE0ZWY5IiwiY2xpZW50X2lkIjoib3Blbi1hcGkifQ.ObNVtPzX2ih6qRt2SPGb_xIQXX_lHzB47NhzHAXkKfJ4Kk70C6W0bYijZCifVDdKUclqsGUv29IhWhj3jIk0yzbGjLYq5YkYMipqh3ZS1wF_D_ehG3Rc17cuOeiY1q4Exmd8oaLU-fNDAbInkFhdqsZBf51Lh-Ytu1zCaxUnPLtLTK-9QZxVAYMnTt9nDupwrR62gsomtUSSOVQgitwpVElf7SJjvpM2_Hv30t9BkpEnXIvr9xrcOUmEg6OE3-evIUS1ymN-v23oxSbgLxoHtpDOdrpQ6BdAz-4CQdUn0U1q7qx8Q6vCf7inDWP8bJXRlKl2aP-B3ou1PWbhE-IwVw',
                'Content-Type': 'multipart/form-data',
                ...formData.getHeaders()
            },
            data: formData
        };

        axios.request(config)
            .then((response) => {
                console.log('PDF sent to Ivia BOL API');
                resolve();
            })
            .catch((error) => {
                console.error('Error sending PDF to Ivia BOL API:', error);
                reject(error);
            });
    });
}
