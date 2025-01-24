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
server.use(bodyParser.urlencoded({ extended: true }));

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
    res.send(data);
});

// Other existing endpoints...

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

server.post('/mysql/filter', async (req, res) => {
    let connection;
    try {
        connection = await db_mysql.getConnection();
        const { agent_name, process_name, campaign_name, filter_date } = req.body;


        // Build query dynamically
        let query = 'SELECT * FROM reports WHERE 1=1';
        let queryParams = [];

        if (agent_name) {
            query += ' AND agent_name LIKE ?';
            queryParams.push(`${agent_name}`); // Using LIKE with wildcards for partial matches
        }
        if (process_name) {
            query += ' AND process_name LIKE ?';
            queryParams.push(`${process_name}`);
        }
        if (campaign_name) {
            query += ' AND campaign_name LIKE ?';
            queryParams.push(`${campaign_name}`);
        }
        if (filter_date) {
            query += ' AND DATE(filter_date) = ?';
            queryParams.push(filter_date);
        }

        query += ' LIMIT 1000';

        // Log the constructed query and parameters for debugging
        console.log('Query:', query);
        console.log('Query Params:', queryParams);

        const [data] = await connection.query(query, queryParams);
        res.send(data);
    } catch (error) {
        res.json({ message: error.message });
    } finally {
        if (connection) connection.release();
    }
});


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
                    hour: {
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

server.post("/mongo/filter", async (req, res) => {
    const { agent_name, campaign_name, process_name } = req.body;
    try {
        const query = {};
        if (agent_name) {
            query.agent_name = { $regex: agent_name, $options: 'i' };
        } if (campaign_name) {
            query.campaign_name = { $regex: campaign_name, $options: 'i' };
        }
        if (process_name) {
            query.process_name = { $regex: process_name, $options: 'i' };
        }

        const results = await collection.find(query).toArray();
        res.send(results);

    } catch (err) {
        res.send(400, { message: err.message });
    }
})



const elasticClient = new Client({
    node: process.env.ELASTIC_URL,
})



server.get('/elastic/getall', async (req, res) => {
    try {
        let response = await elasticClient.search({
            index: 'vikashk21',
            body: {
                query: {
                    match_all: {}
                },
            },
            "size": 10000,
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

server.get('/elastic/getsummary/hourlyreport', async (req, res) => {
    try {
        const response = await elasticClient.search({
            index: 'vikashk21',
            body: {
                "size": 0,
                aggs: {
                    hour: {
                        date_histogram: {
                            field: "data.date_time",
                            calendar_interval: "hour",
                        },
                        aggs: {
                            totalHoldTime: {
                                sum: {
                                    field: "data.hold"
                                }
                            },
                            totalTalkTime: {
                                sum: {
                                    field: "data.callkey"
                                }
                            },
                            totalDisposeTime: {
                                sum: {
                                    field: "data.dispose_time"
                                }
                            },
                            totalDuration: {
                                sum: {
                                    field: "data.duration"
                                }
                            },
                            totalMuteTime: {
                                sum: {
                                    field: "data.mute"
                                }
                            },
                            totalConferenceTime: {
                                sum: {
                                    field: "data.conference"
                                }
                            },
                            totalProcesses: {
                                cardinality: {
                                    field: "data.process_name.keyword"
                                }
                            },
                            totalCampaigns: {
                                cardinality: {
                                    field: "data.campaign_name.keyword"
                                }
                            },
                            totalCalls: {
                                value_count: {
                                    field: "data.date_time"
                                }
                            },
                        }
                    }
                }
            }
        });

        // const formatTime = (seconds) => {
        //     const hours = Math.floor(seconds / 3600);
        //     const minutes = Math.floor((seconds % 3600) / 60);
        //     const secs = seconds % 60;
        //     return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
        // };

        const formatDate = (dateString) => {
            const date = new Date(dateString);
            const year = date.getFullYear();
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
            const hours = String(date.getHours()).padStart(2, '0');
            const minutes = String(date.getMinutes()).padStart(2, '0');
            const seconds = String(date.getSeconds()).padStart(2, '0');
            return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
        };
        // Format the response
        const formattedData = response.aggregations.hour.buckets.map(bucket => ({
            hour: formatDate(bucket.key_as_string),
            totalCalls: bucket.totalCalls.value,
            totalHoldTime: bucket.totalHoldTime.value,
            totalTalkTime: bucket.totalTalkTime.value,
            totalDisposeTime: bucket.totalDisposeTime.value,
            totalDuration: bucket.totalDuration.value,
            totalMuteTime: bucket.totalMuteTime.value,
            totalConferenceTime: bucket.totalConferenceTime.value,
            totalProcesses: bucket.totalProcesses.value,
            totalCampaigns: bucket.totalCampaigns.value
        }));

        res.send(formattedData);
    } catch (error) {
        console.log("Error in the query", error);
        res.status(500).send("Error in the query");
    }
});

server.post('/elastic/filter', async (req, res) => {
    const { agent_name, campaign_name, process_name } = req.body;
    try {
        const query = {
            query: {
                bool: {
                    must: []
                }
            }
        };

        if (agent_name) {
            query.query.bool.must.push({
                match: { "data.agent_name": agent_name }
            });
        }
        if (campaign_name) {
            query.query.bool.must.push({
                match: { "data.campaign_name": campaign_name }
            });
        }
        if (process_name) {
            query.query.bool.must.push({
                match: { "data.process_name": process_name }
            });
        }

        const response = await elasticClient.search({
            index: 'vikashk21',
            body: query,
            size: 10000
        });
        if (response && response.hits && response.hits.hits) {
            res.send(response.hits.hits.map(hit => hit._source.data));
        } else {
            res.send(200, []);
        }
    } catch (error) {
        console.error('Elasticsearch error:', error);
        res.send(500, { error: 'Internal server error' });
    }
});


const PORT = process.env.PORT;
const HOST = process.env.HOST;
server.listen(PORT, () => console.log(`Server listening on ${PORT}:${HOST}`));