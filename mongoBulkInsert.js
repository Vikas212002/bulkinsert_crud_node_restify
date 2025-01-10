const { MongoClient } = require('mongodb');
const { v4: uuidv4 } = require('uuid');
const restify = require('restify');
const server = restify.createServer();

const bodyParser = require('body-parser');
const dotenv = require('dotenv');
dotenv.config();

server.use(bodyParser.json());

//mongodb connection 
const url = "mongodb://localhost:27017";
const client = new MongoClient(url);

const dbname = "nodeCrud";
const collectionname = "bulkdata";
// async function connectMongoAndInsert() {
//     try {
//     await client.connect();
//     console.log("Connected to MongoDB");
//     const db = client.db(dbname);
//     const collection = db.collection(collectionname);

//     const bulkdata = [];
//     for (let i=0; i <= 1000000; i++){
//         const data ={
//             "id": uuidv4(),
//             name: `User_${i}`,
//               age: Math.floor(Math.random() * 60) + 18, // Random age between 18 and 77
//               city: ['New York', 'London', 'Paris', 'Tokyo', 'Sydney'][Math.floor(Math.random() * 5)],
//               timestamp: Date.now(),
//         }
//         bulkdata.push(data);
//     }
//     const result = await collection.insertMany(bulkdata);
//     console.log(`${result.insertedCount} documents were inserted`);
//     }
//     finally{
//         await client.close;
//     }

//     // return db;
// }
// connectMongoAndInsert()


server.get('/get', async (req, res) => {
    await client.connect();
    console.log("Connected to MongoDB");
    const db = client.db(dbname);
    const collection = db.collection(collectionname);
    const result = await db.collection(collection).find({}).toArray();
    res.json(result);

})

const PORT = process.env.PORT;
const HOST = process.env.HOST;
server.listen(PORT, () => console.log(`Server Listening on Port ${PORT}:${HOST}`));


