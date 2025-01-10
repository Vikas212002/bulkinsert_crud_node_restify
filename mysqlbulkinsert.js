
const mysql = require('mysql2/promise');

const restify = require('restify');
const server = restify.createServer();

const bodyParser = require('body-parser');


const dotenv = require('dotenv');
dotenv.config();

server.use(bodyParser.json());

const db = mysql.createPool({
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'bulkdata',
});

//Code for bulk insertion of data into the database

// async function bulkinsertmysql() {
//     try {
//         const connection = await db.getConnection();
//         await connection.beginTransaction();

//         const bulkdata = [];
//         for (let i = 0; i < 1000000; i++) {
//             const data = {
//                 name: `User_${i}`,
//                 age: Math.floor(Math.random() * 60) + 18, // Random age between 18 and 77
//                 city: ['New York', 'London', 'Paris', 'Tokyo', 'Sydney'][Math.floor(Math.random() * 5)],
//                 role: ['Software Developer', 'Devops Engineer', 'Cyber Specialist', 'Buisnessman', 'Chartered Accountant(CA)'][Math.floor(Math.random() * 5)],
//                 gender: ['Male', 'Female', 'Others'][Math.floor(Math.random() * 3)],    
//                 timestamp: Date.now(),
//             }
//             bulkdata.push(data);
//         }

//         const batchSize = 1000;
//         for (let i = 0; i < bulkdata.length; i += batchSize) {
//             const batch = bulkdata.slice(i, i + batchSize);
//             const values = batch.map(data => [data.name, data.age, data.city, data.role, data.gender, data.timestamp]);
//             await connection.query(
//                 `INSERT INTO users (name, age, city, role, gender, timestamp) VALUES ?`,
//                 [values]
//             );
//         }

//         await connection.commit();
//         connection.release();
//         console.log('Bulk insert completed successfully.');

//     } catch (err) {
//         console.error(err);
//     }
// }

// bulkinsertmysql();

server.get('/get/:role', async(req, res) => {
    const role = req.params.role;
    const result = await db.query(`SELECT * FROM users WHERE role = ?` , [role]);
    res.json(result);
})

server.get('/getgender/:gender', async(req, res) => {
    const gender = req.params.gender;
    const result = await db.query(`SELECT * FROM users WHERE gender = ?` , [gender]);
    res.json(result);
})

server.get('/getcity/:city', async(req, res) => {
    const city = req.params.city;
    const result = await db.query(`SELECT * FROM users WHERE city = ?` , [city]);
    res.json(result);
})

server.get('/getcityrole/:city/:role', async(req, res) => {
    const city = req.params.city;
    const role = req.params.role;
    const [result] = await db.query(`SELECT * FROM users WHERE city = ? AND role = ?` , [city, role]);
    console.log(`No of users with city ${city} and role ${role} is ${result.length}`);
    res.json(result);
})

server.get('/getcitygender/:city/:gender', async(req, res) => {
    const city = req.params.city;
    const gender = req.params.gender;
    const [result] = await db.query(`SELECT * FROM users WHERE city = ? AND gender = ?` , [city, gender]);
    console.log(`No of users with city ${city} and gender ${gender} is ${result.length}`);
    res.json(result);
})

server.get('/getrolegender/:role/:gender', async(req, res) => {
    const role = req.params.role;
    const gender = req.params.gender;
    const [result] = await db.query(`SELECT * FROM users WHERE role = ? AND gender = ?` , [role, gender]);
    console.log(`No of users with role ${role} and gender ${gender} is ${result.length}`);
    res.json(result);  
})

server.get('/getall/:role/:city/:gender', async(req, res)=> {
    const role = req.params.role;
    const city = req.params.city;
    const gender = req.params.gender;

    const [result] = await db.query(`SELECT * FROM users WHERE role = ? AND city = ? AND gender = ?` , [role, city, gender]);

    console.log(`No of users for requested query ${result.length}`);
    res.json(result);
})




const PORT = process.env.PORT;
const HOST = process.env.HOST;
server.listen(PORT, ()=> console.log(`Server Listening on Port ${PORT}:${HOST}`) );

