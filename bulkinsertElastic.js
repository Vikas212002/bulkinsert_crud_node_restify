const { Client } = require('@elastic/elasticsearch');
const { v4: uuidv4 } = require('uuid');
const { faker } = require('@faker-js/faker');
const bodyParser = require('body-parser');

const restify = require('restify');
const server = restify.createServer();
server.use(bodyParser.json());

const dotenv = require('dotenv');
dotenv.config();

const elasticClient = new Client({
  node: process.env.ELASTIC_URL,
})

async function bulkinsertelastic() {
  try {
    const bulkdata = [];
    const agents = [
      'John', 'Ramesh', 'Suresh', 'Rohan', 'Mohan',
      'Sohan', 'Rohit', 'Mohit', 'Rahul', 'Raj'
    ];

    const campaigns = [
      'Airtel', 'VI', 'JIO', 'NoBroker', 'Campaign5'
    ];

    const process = [
      'Sales', 'Collection', 'Credit Card', 'Loan', 'FD'
    ];

    let disposeType, disposeName, hold, mute, ringing, transfer, conference, call, duration;
    for (let i = 0; i < 100000; i++) {

      let reportType = ['dispose', 'missed', 'autofailed', 'autodrop'][Math.floor(Math.random() * 4)];

      if (reportType === 'dispose') {
        disposeType = ['call back', 'dnc', 'etx'][Math.floor(Math.random() * 3)];
        disposeName = ['External', 'Transfer', 'Do not call', 'Follow up'][Math.floor(Math.random() * 4)];
        hold = Math.floor(Math.random() * 300000);
        mute = Math.floor(Math.random() * 300000);
        ringing = Math.floor(Math.random() * 300000);
        transfer = Math.floor(Math.random() * 300000);
        conference = Math.floor(Math.random() * 300000);
        call = Math.floor(Math.random() * 300000);
        duration = hold + mute + ringing + transfer + conference + call;
      } else {
        disposeType = 'NULL';
        disposeName = ['External', 'Transfer'][Math.floor(Math.random() * 2)];
        hold = 0;
        mute = 0;
        ringing = Math.floor(Math.random() * 300000);
        transfer = 0;
        conference = 0;
        call = 0;
        duration = 0;
      }

      const data = {
        date_time: faker.date.past(),
        type: reportType,
        dispose_type: disposeType,
        dispose_name: disposeName,
        duration: duration,
        agent_name: agents[Math.floor(Math.random() * agents.length)],
        campaign_name: campaigns[Math.floor(Math.random() * campaigns.length)],
        process_name: process[Math.floor(Math.random() * process.length)],
        leadset: uuidv4(),
        reference_uuid: uuidv4(),
        customer_uuid: uuidv4(),
        hold: hold,
        mute: mute,
        ringing: ringing,
        transfer: transfer,
        conference: conference,
        callkey: call,
        dispose_time: Math.floor(Math.random() * 300000),
      };
      // Add the index operation and data to bulkData
      bulkdata.push({ index: { _index: 'vikashk21' } });
      bulkdata.push({ data });

    }
    // console.log(bulkdata);
    const response = await elasticClient.bulk({ body: bulkdata });
    console.log(response);
    if (response) {
      console.log(`Inserted ${bulkdata.length} records into the index`);
    }

  }
  catch (err) {
    console.log(err);
  }
}

bulkinsertelastic();




// New endpoint to retrieve all documents
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
      scroll: '1m',
    });
    // let documents = response.hits.hits.map(hit => hit._source.data);

    // res.json({ response: documents });

    const scroll_id = response._scroll_id;
    const hits = response.hits.hits.map(hit => hit._source.data);
  
    // Fetch the next set of results
    const nextResponse = await elasticClient.scroll({ scroll_id, scroll: '1m' });
    const nextHits = nextResponse.hits.hits.map(hit => hit._source.data);

    await elasticClient.clearScroll({ scroll_id });
    // res.json({ hits: hits, nextHits: nextHits });
    res.send(hits,nextHits);

  } catch (error) {
    res.json({ message: "Error retrieving all documents", error: error.message });
  }
});

// server.get('/elastic/getall', async (req, res) => {
//   try {
//     const page = req.query.page || 1;
//     const size = req.query.size || 10;
//     const from = (page - 1) * size;
//     let response = await elasticClient.search({
//       index: 'vikask21',
//       body: {
//         "size": 10,
//         "from": from,
//         query: {
//           match_all: {}
//         }, 
//       },
//       "scroll": '5m',
//     });

//     const scroll_id = response._scroll_id;
//     let allHits = response.hits.hits.map(hit => hit._source.data);

//     while (true) {
//       const nextResponse = await elasticClient.scroll({ scroll_id, scroll: '5m' });
//       const nextHits = nextResponse.hits.hits.map(hit => hit._source.data);

//       if (nextHits.length === 0) {
//         break;
//       }

//       allHits = allHits.concat(nextHits);
//     }

//     await elasticClient.clearScroll({ scroll_id });
//     res.json({ hits : allHits });

//   } catch (error) {
//     res.json({ message: "Error retrieving all documents", error: error.message });
//   }
// });

server.get('/elastic/getalldocs', async (req, res) => {
  try {
    const page = req.query.page || 1;
    const size = req.query.size || 10;
    const from = (page - 1) * size;

    let response = await elasticClient.search({
      index: 'vikask21',
      body: {
        "size": size,
        "from": from,
        query: {
          match_all: {}
        },
      },
    });

    const hits = response.hits.hits.map(hit => hit._source.data);

    res.json({ hits: hits, total: response.hits.total.value });

  } catch (error) {
    res.json({ message: "Error retrieving all documents", error: error.message });
  }
});




// server.get('/elastic/getsummary/bycampaign', async (req, res) => {
//   try {
//     const body = {

//       "aggs": {
//         "by_campaign": {
//           "terms": { "field": "campaign_name.keyword" },
//           "aggs": {
//             "total_calls": { "value_count": { "field": "call_id" } },
//             "call_answered": {
//               "filter": {
//                 "term": { "type": "dispose" }
//               }
//             },
//             "missed_calls": {
//               "filter": {
//                 "term": { "type": "missed" }
//               }
//             },
//             "call_autodrop": {
//               "filter": {
//                 "term": { "type": "autodrop" }
//               }
//             },
//             "call_autofail": {
//               "filter": {
//                 "term": { "type": "autofailed" }
//               }
//             },
//             "talktime": { "sum": { "field": "duration" } }
//           }
//         }
//       }
//     };

//     // console.log("Sending query to Elasticsearch:", JSON.stringify(body, null, 2));

//     const result = await elasticClient.search({
//       index: 'vikask21',
//       body
//     });

//     // console.log("Elasticsearch response:", JSON.stringify(result, null, 2));

//     if (!result.aggregations || !result.aggregations.by_campaign) {
//       throw new Error("No aggregations found in the response. Check the index and field names.");
//     }

//     const summary = result.aggregations.by_campaign.buckets.map(bucket => ({
//       campaign_name: bucket.key,
//       total_calls: bucket.total_calls.value,
//       call_answered: bucket.call_answered.doc_count,
//       missed_calls: bucket.missed_calls.doc_count,
//       call_autodrop: bucket.call_autodrop.doc_count,
//       call_autofail: bucket.call_autofail.doc_count,
//       talktime: bucket.talktime.value
//     }));
//     // console.log(summary);

//     res.json({ message: "Success", response: summary });
//   } catch (error) {
//     console.error("Error in /elastic/getsummary/bycampaign:", error);
//     res.json({ message: "Error fetching summary", error: error.message });
//   }
// });

// server.get('/elastic/getsummary/bycampaign', async (req, res) => {
//   try {
//     const body = {
//       "aggs": {
//         "by_campaign": {
//           "terms": {
//             "field": "campaign_name.keyword",
//             "size": 1000
//           },
//           "aggs": {
//             "total_calls": { "value_count": { "field": "call_id" } },
//             "call_answered": {
//               "filter": {
//                 "term": { "type": "dispose" }
//               }
//             },
//             "missed_calls": {
//               "filter": {
//                 "term": { "type": "missed" }
//               }
//             },
//             "call_autodrop": {
//               "filter": {
//                 "term": { "type": "autodrop" }
//               }
//             },
//             "call_autofail": {
//               "filter": {
//                 "term": { "type": "autofailed" }
//               }
//             },
//             "talktime": { "sum": { "field": "duration" } }
//           }
//         }
//       }
//     };

//     console.log('Searching index...');
//     const result = await elasticClient.search({
//       index: 'vikask21',
//       body
//     });
//     console.log('Search result:', result);

//     if (!result.aggregations || !result.aggregations.by_campaign) {
//       throw new Error("No aggregations found in the response. Check the index and field names.");
//     }

//     const summary = result.aggregations.by_campaign.buckets.map(bucket => ({
//       campaign_name: bucket.key,
//       total_calls: bucket.total_calls.value,
//       call_answered: bucket.call_answered.doc_count,
//       missed_calls: bucket.missed_calls.doc_count,
//       call_autodrop: bucket.call_autodrop.doc_count,
//       call_autofail: bucket.call_autofail.doc_count,
//       talktime: bucket.talktime.value
//     }));

//     console.log('Summary:', summary);
//     res.json({ message: "Success", response: summary });
//   } catch (error) {
//     console.error("Error in /elastic/getsummary/bycampaign:", error);
//     res.json({ message: "Error fetching summary", error: error.message });
//   }
// });

server.get('/elastic/getsummary/bycampaign', async (req, res) => {
  try {
    const body = {
      "aggs": {
        "by_campaign": {
          "terms": {
            "field": "campaign_name.keyword",
            "size": 1000
          },
          "aggs": {
            "total_calls": { "value_count": { "field": "call_id" } },
            "call_answered": {
              "filter": {
                "term": { "type": "dispose" }
              }
            },
            "missed_calls": {
              "filter": {
                "term": { "type": "missed" }
              }
            },
            "call_autodrop": {
              "filter": {
                "term": { "type": "autodrop" }
              }
            },
            "call_autofail": {
              "filter": {
                "term": { "type": "autofailed" }
              }
            },
            "talktime": { "sum": { "field": "duration" } }
          }
        }
      }
    };

    console.log('Searching index...');
    const result = await elasticClient.search({
      index: 'vikask21',
      body
    });
    console.log('Search result:', result);

    if (!result.aggregations || !result.aggregations.by_campaign) {
      throw new Error("No aggregations found in the response. Check the index and field names.");
    }

    console.log('Aggregation result:', result.aggregations.by_campaign);
    console.log('Buckets:', result.aggregations.by_campaign.buckets);

    const summary = result.aggregations.by_campaign.buckets.map(bucket => ({
      campaign_name: bucket.key,
      total_calls: bucket.total_calls.value,
      call_answered: bucket.call_answered.doc_count,
      missed_calls: bucket.missed_calls.doc_count,
      call_autodrop: bucket.call_autodrop.doc_count,
      call_autofail: bucket.call_autofail.doc_count,
      talktime: bucket.talktime.value
    }));

    console.log('Summary:', summary);
    res.json({ message: "Success", response: summary });
  } catch (error) {
    console.error("Error in /elastic/getsummary/bycampaign:", error);
    res.json({ message: "Error fetching summary", error: error.message });
  }
});

server.get('/elastic/callreportSummary/get', async (req, res) => {
  try {
    const result = await elasticClient.search({
      index: 'vikask21',
      body: {
        aggs: {
          by_hour: {
            date_histogram: {
              field: 'date_time',
              fixed_interval: '1h'
            },
            aggs: {
              call_count: {
                value_count: {
                  field: 'date_time'
                }
              },
              AnsweredCount: {
                filter: { term: { "calltype.keyword": "dispose" } },
                aggs: {
                  call_count: {
                    value_count: {
                      field: 'date_time'
                    }
                  }
                }
              },
              dropCount: {
                filter: { term: { "calltype.keyword": "autodrop" } },
                aggs: {
                  call_count: {
                    value_count: {
                      field: 'date_time'
                    }
                  }
                }
              },
              failCount: {
                filter: { term: { "calltype.keyword": "autofail" } },
                aggs: {
                  call_count: {
                    value_count: {
                      field: 'date_time'
                    }
                  }
                }
              },
              missedCount: {
                filter: { term: { "calltype.keyword": "missed" } },
                aggs: {
                  call_count: {
                    value_count: {
                      field: 'date_time'
                    }
                  }
                }
              },
              Talktime: {
                sum: { field: 'duration' }
              },
            }
          }
        },
        size: 0,
      }
    });

    const resultarray = result.aggregations.by_hour.buckets;
    console.log("Aggregation result:", JSON.stringify(resultarray, null, 2));

    res.send({ response: resultarray });
  } catch (error) {
    console.error(error);
    res.send({ message: "Error retrieving call report summary", error: error.message });
  }
});

const PORT = process.env.PORT;
const HOST = process.env.HOST;
server.listen(PORT, () => console.log(`Server listening on ${PORT}:${HOST}`));
