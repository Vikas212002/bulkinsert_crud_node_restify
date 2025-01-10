// const redis = require('ioredis');
// const dotenv = require('dotenv');

// // const redis = new Redis();
// dotenv.config();

// const client = redis.createClient({
//     host: process.env.REDIS_IP,
//     port: process.env.REDIS_PORT
// })
// client.on('error', err => console.log('Redis Client Error', err));

// if (client) {
//     console.log('Redis Client Connected');
// }

// async function bulkinsertredis() {
//     const bulkdata = [];
//     for (let i = 0; i < 10000; i++) {
//         const data = {
//             "id": uuidv4(),
//             name: `User_${i}`,
//             age: Math.floor(Math.random() * 60) + 18, // Random age between 18 and 77
//             city: ['New York', 'London', 'Paris', 'Tokyo', 'Sydney'][Math.floor(Math.random() * 5)],
//             timestamp: Date.now(),
//         }
//         bulkdata.push(data);
//     }
   
//     const bulk = client.pipeline();
//     for (let i = 0; i < bulkdata.length; i++) {
//         const data = JSON.stringify(bulkdata[i]);
//         bulk.hset(`user:${bulkdata[i].id}`, data);
//         bulk.expire(`user:${bulkdata[i].id}`, 3600); // expire in
//     }
//     await bulk.exec();
//     console.log('Bulk Inserted in Redis');
//     console.log(bulkdata.length);

//     async function countKeys(pattern) {
//         let cursor = '0';
//         let count = 0;
//         do {
//             const [newCursor, keys] = await client.scan(cursor, 'MATCH', pattern, 'COUNT', '1000');
//             cursor = newCursor;
//             count += keys.length;
//         } while (cursor !== '0');
//         return count;
//     }
    
//     countKeys('user:*').then(total => {
//         console.log(`Total keys inserted: ${total}`);
//     }).catch(err => {
//         console.error('Error fetching keys:', err);
//     });
    
// }
// bulkinsertredis();
    




const redis = require('ioredis');
const { faker } = require('@faker-js/faker');
const { v4: uuidv4 } = require('uuid');

require('dotenv').config(); 

const redisClient = redis.createClient({
    host: process.env.REDIS_IP,
    port: process.env.REDIS_PORT
});

// Handle Redis connection errors
redisClient.on('error', (err) => {
    console.error('Redis error:', err);
});


function bulkInsertUsers() {
    const users = [];
    for (let i = 0; i < 800000; i++) {
        const email = faker.internet.email();
        users.push({
            key: `user:${email}`,
            "id": uuidv4(),
            name: `User_${i}`,
            age: Math.floor(Math.random() * 60) + 18,
            city: ['New York', 'London', 'Paris', 'Tokyo', 'Sydney'][Math.floor(Math.random() * 5)],
            timestamp: Date.now(),
        });
    }
    const pipeline = redisClient.pipeline();
    users.forEach(user => {
        pipeline.hset(user.key, 'firstname', user.firstname, 'lastname', user.lastname, 'email', user.email, 'password', user.password);
    });
    console.log(pipeline);
    return pipeline.exec().then(() => {
        console.log(`Inserted ${users.length} users`);
        // console.log(users.count)
    }).catch(error => {
        console.error('Failed to bulk insert users:', error.message);
    });
}
bulkInsertUsers();

