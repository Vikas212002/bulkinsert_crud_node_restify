const restify = require('restify');
const bodyParser = require('body-parser');
const config = require('./config/config');
const dotenv = require('dotenv');
const server = restify.createServer();
const redis = require('ioredis');

const { Client } = require('@elastic/elasticsearch');


const { v4: uuidv4 } = require('uuid');
var userid = uuidv4()

dotenv.config();

server.use(bodyParser.json());

//mysql

server.post('/create', async (req, res) => {
  const { name, email, age } = req.body;
  if (name === undefined || email === undefined || age === undefined) {
    res.send(400, { error: 'Name, email, and age are required.' });
  }

  const result = await config.db.execute('INSERT INTO users (name, email, age) VALUES (?, ?, ?)', [name, email, age]);
  res.json(result);
});

server.get('/read/:id', async (req, res) => {
  const [result] = await config.db.execute('SELECT * FROM users WHERE id = ?', [req.params.id]);
  res.json(result);
});

server.put('/update/:id', async (req, res) => {
  const { name, email, age } = req.body;
  const [result] = await config.db.execute(
    'UPDATE users SET name = ?, email = ?, age = ? WHERE id = ?',
    [name, email, age, req.params.id]
  );
  res.json(result);
});

server.del('/delete/:id', async (req, res) => {
  const [result] = await config.db.execute('DELETE FROM users WHERE id = ?', [req.params.id]);
  res.json(result);
});

server.post('/mysql/bulkpush', async (req, res)=>{
  

})

//CRUD using Mongodb

server.post('/mongo/create', async (req, res) => {
  const { name, email, age } = req.body;
  if (name === undefined || email === undefined || age === undefined) {
    return res.send(400, { error: 'Name, email, and age are required.' });
  }

  const db = await config.connectMongoDB();
  const result = await db.collection('crudNodeApi').insertOne({ userid, name, email, age });
  console.log('Inserted document =>', result);
  res.json(result);
});

server.get('/mongo/read/:name', async (req, res) => {
  const db = await config.connectMongoDB();
  const name = req.params.name;
  const result = await db.collection('crudNodeApi').find({ name: name }).toArray();
  console.log(result);
  if (!result) {
    return res.send(404, { error: 'User not found.' });
  }
  res.json(result);
});

server.put('/mongo/update/:name', async (req, res) => {
  const { name, email, age } = req.body;
  const db = await config.connectMongoDB();
  const username = req.params.name;

  const result = await db.collection('crudNodeApi').updateOne(
    { name: username },
    { $set: { name, email, age } }
  );
  if (result.matchedCount === 0) {
    return res.send(404, { error: 'User not found.' });
  }
  res.json({ message: 'User updated successfully.' });
});

server.del('/mongo/delete/:name', async (req, res) => {
  const db = await config.connectMongoDB();
  const username = req.params.name;
  const result = await db.collection('crudNodeApi').deleteOne({ name: username });
  if (result.deletedCount === 0) {
    return res.send(404, { error: 'User not found.' });
  }
  res.json({ message: 'User deleted successfully.' });
});

//Redis

const client = redis.createClient({
  host: process.env.REDIS_IP,
  port: process.env.REDIS_PORT
})

client.on('error', err => console.log('Redis Client Error', err));

server.post('/redis/create', async (req, res) => {
  const { name, email, age } = req.body;
  try {
    await client.hset(name, 'name', name, 'email', email, 'age', age);
    res.json({ message: 'User created successfully.' });
  } catch (error) {
    console.error('Error creating user in Redis:', error);
    res.send(500).json({ error: 'Failed to create user in Redis.' });
  }
})

server.get('/redis/get/:name', async (req, res) => {
  const { name } = req.params;

  try {
    const user = await client.hgetall(name);
    if (!user) {
      return res.status(404).json({ error: 'User not found.' });
    }
    res.json({ user });
  } catch (error) {
    console.error('Error reading user from Redis:', error);
    res.status(500).json({ error: 'Failed to read user from Redis.' });
  }
})

server.del('/redis/delete/:name', async (req, res) => {
  const { name } = req.body;
  try {
    await client.hdel(name, 'name', 'email', 'age');
    res.json({ message: 'User deleted successfully.' });
  } catch (error) {
    console.error('Error deleting user in Redis:', error);
    res.send(500).json({ error: 'Failed to delete user in Redis.' });
  }
})

//Elastic

const elasticClient = new Client({
  node: process.env.ELASTIC_URL
});


server.post('/elastic/create', async (req, res) => {
  const { name, email, age } = req.body;
  try {
    const result = await elasticClient.index({
      index: 'users',
      document: {
        name,
        email,
        age
      }
    });
    res.json({ message: 'User created successfully in Elasticsearch.', result });
  } catch (error) {
    console.error('Error creating user in Elasticsearch:', error);
    res.status(500).json({ error: 'Failed to create user in Elasticsearch.' });
  }
});

// Read all user from Elasticsearch
server.get('/elastic/get/:index', async (req, res) => {
  const { index } = req.params;
  try {
    const response = await elasticClient.search({ index });
    res.send(response);
  } catch (error) {
    console.error('Error reading user from Elasticsearch:', error.message);
    res.send({ error: 'User not found in Elasticsearch.' });
  }
});

// Read a  single user from Elasticsearch
server.get('/elastic/single/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const response = await elasticClient.get({
      index: 'users',
      id: id
    });
    res.send(response);
  } catch (error) {
    console.error('Error reading user from Elasticsearch:', error.message);
    res.send({ error: 'User not found in Elasticsearch.' });
  }
});

// Update a user in Elasticsearch
server.put('/elastic/update/:id', async (req, res) => {
  const { id } = req.params;
  const { name, email, age } = req.body;
  try {
    const result = await elasticClient.update({
      index: 'users',
      id,
      body: {
        doc: {
          name,
          email,
          age
        }
      }
    });
    res.send(result);
  } catch (error) {
    res.send(error);
  }
});

// Delete a single user from Elasticsearch
server.del('/elastic/delete/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const result = await elasticClient.delete({
      index: 'users',
      id
    });
    res.send(result);
  } catch (error) {
    res.send(error);
  }
});

// Delete a index user from Elasticsearch
server.del('/elastic/deleteall/:index/:id', async (req, res) => {
  const { index, id } = req.params;
  try {
    const result = await elasticClient.delete({
      index,
      id
    });
    res.send(result);
  } catch (error) {
    res.send(error);
  }
});

//Bulk push in elastic
const BATCH_SIZE = 1000;
server.post("/elastic/bulkpush", async (req, res) => { 
  let bulkData = []; 
  for (let i = 0; i < 300000; i+=BATCH_SIZE) { 
    const randomData = {
              id: i,
              name: `User_${i}`,
              age: Math.floor(Math.random() * 60) + 18, // Random age between 18 and 77
              city: ['New York', 'London', 'Paris', 'Tokyo', 'Sydney'][Math.floor(Math.random() * 5)],
              timestamp: Date.now(),
            };
    
    // Add the index operation and data to bulkData
    bulkData.push({ index: { _index: 'vikash' } }); 
    bulkData.push({ randomData }); 
  } 

  // console.log(bulkData);

  try { 
    const response = await elasticClient.bulk({ body: bulkData }); 

    if (response.errors) { 
      res.json({ 
        message: "Failed to save some or all documents", 
        errors: response.items.filter(item => item.index && item.index.error) 
      }); 
    } else { 
      res.json({ 
        message: "Successfully Added", 
        data: response 
      }); 
    } 
  } catch (error) { 
    res.json({ message: "Failed to save", error: error.message });
  }
});

server.listen(process.env.PORT, () => console.log(`Server is running on port ${process.env.PORT}:${process.env.HOST}`));
