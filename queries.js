// queries.js - MongoDB CRUD, Advanced Queries, Aggregation, and Indexing

const { MongoClient } = require('mongodb');
const uri = 'mongodb://localhost:27017';

async function runQueries() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    const db = client.db('plp_bookstore');
    const books = db.collection('books');
    console.log('Connected to MongoDB!');

    // =======================
    // Task 2: Basic Queries
    // =======================

    console.log(await books.find({ genre: 'Fiction' }).toArray());
    console.log(await books.find({ published_year: { $gt: 2000 } }).toArray());
    console.log(await books.find({ author: 'George Orwell' }).toArray());

    await books.updateOne({ title: '1984' }, { $set: { price: 12.99 } });
    await books.deleteOne({ title: 'Moby Dick' });

    // =======================
    // Task 3: Advanced Queries
    // =======================

    console.log(await books.find({ in_stock: true, published_year: { $gt: 2010 } }).toArray());
    console.log(await books.find({}, { projection: { title: 1, author: 1, price: 1 } }).toArray());

    console.log(await books.find().sort({ price: 1 }).toArray()); // ascending
    console.log(await books.find().sort({ price: -1 }).toArray()); // descending

    console.log(await books.find().skip(0).limit(5).toArray()); // page 1
    console.log(await books.find().skip(5).limit(5).toArray()); // page 2

    // =======================
    // Task 4: Aggregation
    // =======================

    console.log(await books.aggregate([
      { $group: { _id: '$genre', avgPrice: { $avg: '$price' } } }
    ]).toArray());

    console.log(await books.aggregate([
      { $group: { _id: '$author', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 }
    ]).toArray());

    console.log(await books.aggregate([
      { $group: { _id: { $subtract: ['$published_year', { $mod: ['$published_year', 10] }] }, count: { $sum: 1 } } }
    ]).toArray());

    // =======================
    // Task 5: Indexing
    // =======================

    await books.createIndex({ title: 1 });
    await books.createIndex({ author: 1, published_year: -1 });

    console.log(await books.find({ title: '1984' }).explain('executionStats'));

  } catch (err) {
    console.error(err);
  } finally {
    await client.close();
    console.log('Connection closed');
  }
}

runQueries();
