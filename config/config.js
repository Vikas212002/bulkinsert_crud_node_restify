const mysql = require('mysql2/promise');
const { MongoClient } = require('mongodb');
require("dotenv").config({ path: ".elastic.env" });

const dotenv = require('dotenv');
dotenv.config();


// Connection URL
// import { MongoClient } from 'mongodb'

// Connection URL
const url = 'mongodb://localhost:27017';
const client = new MongoClient(url);

// Database Name
const dbName = 'nodeCrud';

async function connectMongoDB() {
  await client.connect();
  console.log('Connected successfully to MongoDB server');
  const db = client.db(dbName);
  return db;
}


module.exports = {
  db: mysql.createPool({
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'nodecrud'
  }),
  connectMongoDB
};

