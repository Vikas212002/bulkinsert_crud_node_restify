const mysql = require('mysql2/promise');
const dotenv = require('dotenv');
const { faker } = require('@faker-js/faker');
const bodyParser = require('body-parser');
const { v4: uuidv4 } = require('uuid');
const { MongoClient } = require('mongodb');
const { Client } = require('@elastic/elasticsearch');

const restify = require('restify');
const server = restify.createServer();
server.use(bodyParser.json());

const bulkinsertion = require('./utils');

dotenv.config();

uuidv4();

const db_mysql = mysql.createPool({
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'bulkdata',
});


server.get('/mysql/getall', async (req, res) => {
    let connection = await db_mysql.getConnection();
    let [data] = await db_mysql.query(`SELECT * from reports limit 10000`);
    connection.release();
    // res.json({ message: "Success", response: data });
    res.send(data);
})

server.get('/sql/getreport/bycampaign', async (req, res) => {

    let connection = await db_mysql.getConnection();

    const [data] = await connection.query(`
          SELECT 
            campaign_name, 
            COUNT(*) AS Total_Calls,
            HOUR(date_time) AS Call_hours,
            DATE_FORMAT(date_time, '%Y-%m-%d %H:00:00.000') AS datetime, 
            SUM(CASE WHEN type = 'dispose' THEN 1 ELSE 0 END) AS Call_Answered,
            SUM(CASE WHEN type = 'missed' THEN 1 ELSE 0 END) AS Missed_Calls,
            SUM(CASE WHEN type = 'autofailed' THEN 1 ELSE 0 END) AS Call_Autodrop, 
            SUM(CASE WHEN type = 'autodrop' THEN 1 ELSE 0 END) AS Call_Autofail, 
            SUM(duration) AS Talktime   
          FROM 
            reports 
          GROUP BY 
          campaign_name
        `);

    connection.release();

    res.json({ message: "Success", response: data });

})


server.get('/sql/getreport/byprocess', async (req, res) => {

    let connection = await db_mysql.getConnection();

    const [data] = await connection.query(`
          SELECT 
            process_name, 
            COUNT(*) AS Total_Calls,
            HOUR(date_time) AS Call_hours,
            DATE_FORMAT(date_time, '%Y-%m-%d %H:00:00.000') AS datetime, 
            SUM(CASE WHEN type = 'dispose' THEN 1 ELSE 0 END) AS Call_Answered,
            SUM(CASE WHEN type = 'missed' THEN 1 ELSE 0 END) AS Missed_Calls,
            SUM(CASE WHEN type = 'autofailed' THEN 1 ELSE 0 END) AS Call_Autodrop, 
            SUM(CASE WHEN type = 'autodrop' THEN 1 ELSE 0 END) AS Call_Autofail, 
            SUM(duration) AS Talktime   
          FROM 
            reports 
          GROUP BY 
          process_name
        `);

    connection.release();

    res.json({ message: "Success", response: data });

})

// Hourly Report
server.get("/mysql/hourlyReport", async (req, res) => {
    let connection = await db_mysql.getConnection();
    try {

        const [data] = await db_mysql.query(`SELECT DATE_FORMAT(date_time, '%Y-%m-%d %H:00:00') as hour,
          COUNT(*) as totalCalls, 
          SUM(hold) as totalHoldTime, 
          SUM(callkey) as totalTalkTime, 
          SUM(dispose_time) as totalDisposeTime, 
          SUM(duration) as totalDuration, 
          SUM(mute) as totalMuteTime, 
          SUM(conference) as totalConferenceTime, 
          COUNT(DISTINCT process_name) as totalProcesses, 
          COUNT(DISTINCT campaign_name) as totalCampaigns 
          FROM
           reports WHERE 1=1
           GROUP BY 
           hour ORDER BY hour limit 1000` );
           
            // const result = [...data];
        res.send(data);
    } catch (err) {
        console.error(err);
        res.send(500, { message: err.message });
    }
    connection.release();
});


//Mongo

//mongodb connection 
const url = "mongodb://localhost:27017";
const client = new MongoClient(url);

const dbname = "nodeCrud";
const collectionname = "bulksummary_reports";

client.connect();
console.log("Connected to MongoDB");

const db = client.db(dbname);
const collection = db.collection(collectionname);

server.get('/mongo/getall', async (req, res) => {
    const result = await collection.find().toArray();
    // res.json({ message: "Success", response: result });
    res.send(result);
})


server.get('/mongo/get/summary', async (req, res) => {
    try {

        const pipeline = [
            {
                $group: {
                    _id: "$process_name", // Group by process_name only
                    total_calls: { $sum: 1 },
                    call_answered: { $sum: { $cond: [{ $eq: ["$type", "dispose"] }, 1, 0] } },
                    missed_calls: { $sum: { $cond: [{ $eq: ["$type", "missed"] }, 1, 0] } },
                    call_autodrop: { $sum: { $cond: [{ $eq: ["$type", "autodrop"] }, 1, 0] } },
                    call_autofail: { $sum: { $cond: [{ $eq: ["$type", "autofailed"] }, 1, 0] } },
                    talktime: { $sum: "$duration" }
                }
            },
            {
                $project: {
                    process_name: "$_id",
                    total_calls: 1,
                    call_answered: 1,
                    missed_calls: 1,
                    call_autodrop: 1,
                    call_autofail: 1,
                    talktime: 1
                }
            }
        ];

        const result = await collection.aggregate(pipeline).toArray();
        res.json({ message: "Success", response: result });
    } catch (error) {
        res.json({ message: "Error fetching summary", error: error.message });
    }
});


server.get('/mongo/getsummary/bycampaign', async (req, res) => {
    try {
        const pipeline = [
            {
                $group: {
                    _id: "$campaign_name",
                    total_calls: { $sum: 1 },
                    call_answered: { $sum: { $cond: [{ $eq: ["$type", "dispose"] }, 1, 0] } },
                    missed_calls: { $sum: { $cond: [{ $eq: ["$type", "missed"] }, 1, 0] } },
                    call_autodrop: { $sum: { $cond: [{ $eq: ["$type", "autodrop"] }, 1, 0] } },
                    call_autofail: { $sum: { $cond: [{ $eq: ["$type", "autofailed"] }, 1, 0] } },
                    talktime: { $sum: "$duration" }
                }
            },
            {
                $project: {
                    campaign_name: "$_id",
                    total_calls: 1,
                    call_answered: 1,
                    missed_calls: 1,
                    call_autodrop: 1,
                    call_autofail: 1,
                    talktime: 1
                }
            }
        ];
        const result = await collection.aggregate(pipeline).toArray();
        res.json({ message: "Success", response: result });
    }
    catch (error) {
        res.json({ message: "Error fetching summary", error: error.message });
    }
})



// server.get("/mongo/getsummary/hourlyreport", async (req, res) => {
//     try {
//         const pipeline = [
//             {
//                 $group: {
//                     _id: {
//                         year: { $year: "$date_time" },
//                         month: { $month: "$date_time" },
//                         day: { $dayOfMonth: "$date_time" },
//                         hour: { $hour: "$date_time" }
//                     },
//                     hour: { $addToSet: "$date_time" },
//                     totalCalls: { $sum: 1 },
//                     totalHoldTime: { $sum: "$hold" },
//                     totalTalkTime: { $sum: "$callkey" },
//                     totalDisposeTime: { $sum: "$dispose_time" },
//                     totalDuration: { $sum: "$duration" },
//                     totalMuteTime: { $sum: "$mute" },
//                     totalConferenceTime: { $sum: "$conference" },
//                     totalProcesses: { $addToSet: "$process_name" },
//                     totalCampaigns: { $addToSet: "$campaign_name" }
//                 }
//             },
//             {
//                 $sort: {
//                     _id: 1
//                 }
//             },
//             {
//                 $project: {
//                     _id: 0,
//                     date: "$_id",
//                     totalCalls: 1,
//                     totalHoldTime: 1,
//                     totalTalkTime: 1,
//                     totalDisposeTime: 1,
//                     totalDuration: 1,
//                     totalMuteTime: 1,
//                     totalConferenceTime: 1,
//                     totalProcesses: { $size: "$totalProcesses" },
//                     totalCampaigns: { $size: "$totalCampaigns" }
//                 }
//             }
//         ];

//         const data = await collection.aggregate(pipeline).toArray();
//         res.send(data);
//     } catch (err) {
//         res.send(500, {
//             message: err.message
//         });
//     }
// });

//elasticsearch

server.get("/mongo/getsummary/hourlyreport", async (req, res) => {
    try {
        const pipeline = [
            {
                $group: {
                    _id: {
                        year: { $year: "$date_time" },
                        month: { $month: "$date_time" },
                        day: { $dayOfMonth: "$date_time" },
                        hour: { $hour: "$date_time" }
                    },
                    totalCalls: { $sum: 1 },
                    totalHoldTime: { $sum: "$hold" },
                    totalTalkTime: { $sum: "$callkey" },
                    totalDisposeTime: { $sum: "$dispose_time" },
                    totalDuration: { $sum: "$duration" },
                    totalMuteTime: { $sum: "$mute" },
                    totalConferenceTime: { $sum: "$conference" },
                    totalProcesses: { $addToSet: "$process_name" },
                    totalCampaigns: { $addToSet: "$campaign_name" }
                }
            },
            {
                $sort: {
                    _id: 1
                }
            },
            {
                $project: {
                    _id: 0,
                    date: {
                        $dateToString: {
                            format: "%Y-%m-%d %H:00:00",
                            date: {
                                $dateFromParts: {
                                    year: "$_id.year",
                                    month: "$_id.month",
                                    day: "$_id.day",
                                    hour: "$_id.hour"
                                }
                            }
                        }
                    },
                    totalCalls: 1,
                    totalHoldTime: 1,
                    totalTalkTime: 1,
                    totalDisposeTime: 1,
                    totalDuration: 1,
                    totalMuteTime: 1,
                    totalConferenceTime: 1,
                    totalProcesses: { $size: "$totalProcesses" },
                    totalCampaigns: { $size: "$totalCampaigns" }
                }
            }
        ];

        const data = await collection.aggregate(pipeline).toArray();
        res.send(data);
    } catch (err) {
        res.send(500, {
            message: err.message
        });
    }
});


const elasticClient = new Client({
    node: process.env.ELASTIC_URL,
})

server.get('/elastic/getall', async (req, res) => {
    try {
        let response = await elasticClient.search({
            index: 'vikask21',
            body: {
                "size": 10000,
                query: {
                    match_all: {}
                },
            },
            scroll: '5m',
        });
        
        const scroll_id = response._scroll_id;
        const hits = response.hits.hits.map(hit => hit._source.data);

        // Fetch the next set of results
        const nextResponse = await elasticClient.scroll({ scroll_id, scroll: '1m' });
        const nextHits = nextResponse.hits.hits.map(hit => hit._source.data);

        const allHits = [...hits, ...nextHits];

        await elasticClient.clearScroll({ scroll_id });
        // res.json({ hits: hits, nextHits: nextHits });
        res.send(allHits);

    } catch (error) {
        res.json({ message: "Error retrieving all documents", error: error.message });
    }
});

const PORT = process.env.PORT;
const HOST = process.env.HOST;
server.listen(PORT, () => console.log(`Server listening on ${PORT}:${HOST}`));