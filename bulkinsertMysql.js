const mysql = require('mysql2/promise');
const dotenv = require('dotenv');
// const { bulkinsertion } = require('./utils'); // Import the function
const { faker } = require('@faker-js/faker');
const bodyParser = require('body-parser');

const restify = require('restify');
const server = restify.createServer();
server.use(bodyParser.json());

const bulkinsertion = require('./utils');

const { v4: uuidv4 } = require('uuid');
dotenv.config();

uuidv4();

const db = mysql.createPool({
  host: 'localhost',
  user: 'root',
  password: '',
  database: 'bulkdata',
});



// async function bulkinsertion() {
//     try {
//         const connection = await db.getConnection();
//         await connection.beginTransaction();

//         const bulkdata = [];
//         const agents = [
//             'John', 'Ramesh', 'Suresh', 'Rohan', 'Mohan',
//             'Sohan', 'Rohit', 'Mohit', 'Rahul', 'Raj'
//         ];

//         const campaigns = [
//             'Airtel', 'VI', 'JIO', 'NoBroker', 'Campaign5'
//         ];

//         const process = [
//             'Sales', 'Collection', 'Credit Card', 'Loan', 'FD'
//         ];

//         let disposeType, disposeName, hold, mute, ringing, transfer, conference, call, duration;
//         for (let i = 0; i < 100000; i++) {

//             let reportType = ['dispose', 'missed', 'autofailed', 'autodrop'][Math.floor(Math.random() * 4)];

//             if (reportType === 'dispose') {
//                 disposeType = ['call back', 'dnc', 'etx'][Math.floor(Math.random() * 3)];
//                 disposeName = ['External', 'Transfer', 'Do not call', 'Follow up'][Math.floor(Math.random() * 4)];
//                 hold = Math.floor(Math.random() * 300000);
//                 mute = Math.floor(Math.random() * 300000);
//                 ringing = Math.floor(Math.random() * 300000);
//                 transfer = Math.floor(Math.random() * 300000);
//                 conference = Math.floor(Math.random() * 300000);
//                 call = Math.floor(Math.random() * 300000);
//                 duration = hold + mute + ringing + transfer + conference + call;
//             } else {
//                 disposeType = 'NULL';
//                 disposeName = ['External', 'Transfer'][Math.floor(Math.random() * 2)];
//                 hold = 0;
//                 mute = 0;
//                 ringing = Math.floor(Math.random() * 300000);
//                 transfer = 0;
//                 conference = 0;
//                 call = 0;
//                 duration = 0;
//             }

//             const data = {
//                 date_time: faker.date.past(),
//                 type: reportType,
//                 dispose_type: disposeType,
//                 dispose_name: disposeName,
//                 duration: duration,
//                 agent_name: agents[Math.floor(Math.random() * agents.length)],
//                 campaign_name: campaigns[Math.floor(Math.random() * campaigns.length)],
//                 process_name: process[Math.floor(Math.random() * process.length)],
//                 leadset: uuidv4(),
//                 reference_uuid: uuidv4(),
//                 customer_uuid: uuidv4(),
//                 hold: hold,
//                 mute: mute,
//                 ringing: ringing,
//                 transfer: transfer,
//                 conference: conference,
//                 callkey: call,
//                 dispose_time: Math.floor(Math.random() * 300000),
//             };
//             bulkdata.push(data);
//         }

//         const insertQuery = 'INSERT INTO reports SET ?';
//         for (let data of bulkdata) {
//             await connection.query(insertQuery, data);
//         }

//         await connection.commit();
//         console.log('Data inserted successfully');
//     } catch (err) {
//         console.log(err);

//     }
// }


// bulkinsertion();

server.get('/mysql/getall', async (req, res) => {
  let connection = await db.getConnection();
  let [data] = await db.query(`SELECT * from reports limit 10000`);
  connection.release();
  // res.json({ message: "Success", response: data });
  res.send(data);
})

server.get('/sql/getreport/bycampaign', async (req, res) => {

  let connection = await db.getConnection();

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

  let connection = await db.getConnection();

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
  let connection = await db.getConnection();
  try {
    
    const [data] = await db.query(`SELECT DATE_FORMAT(date_time, '%Y-%m-%d %H:00:00') as hour,
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

       res.send(200, { response: data });
  } catch (err) {
    console.error(err);
    res.send(500, { message: err.message });
  }
  connection.release();
});


const PORT = process.env.PORT;
const HOST = process.env.HOST;
server.listen(PORT, () => console.log(`Server Listening on Port ${PORT}:${HOST}`));

// module.exports = bulkinsertion;