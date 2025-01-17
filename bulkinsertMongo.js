const { MongoClient } = require('mongodb');
const { v4: uuidv4 } = require('uuid');
const restify = require('restify');
const server = restify.createServer();
const { faker } = require('@faker-js/faker');
const bodyParser = require('body-parser');
const dotenv = require('dotenv');
dotenv.config();

server.use(bodyParser.json());

//mongodb connection 
const url = "mongodb://localhost:27017";
const client = new MongoClient(url);

const dbname = "nodeCrud";
const collectionname = "bulksummary_reports";

client.connect();
console.log("Connected to MongoDB");
const db = client.db(dbname);
const collection = db.collection(collectionname);

// async function connectMongoAndInsert() {
//     try {
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

//         const result = await collection.insertMany(bulkdata);
//         console.log(`${result.insertedCount} documents were inserted`);
//     }
//     finally {
//         await client.close;
//     }

// }
// connectMongoAndInsert()



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
          hour : { $addToSet : "$date_time"},
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
          _id:1
        }
      },
      {
        $project: {
         _id: 0,
         date: "$_id",
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
    res.send(200, {
      hourlyReport: data
    });
  } catch (err) {
    res.send(500, {
      message: err.message
    });
  }
});




const PORT = process.env.PORT;
const HOST = process.env.HOST;
server.listen(PORT, () => console.log(`Server Listening on Port ${PORT}:${HOST}`));

// module.exports = connectMongoAndInsert;

