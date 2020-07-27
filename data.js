'use strict'

require('array.prototype.flatmap').shim()
const { Client } = require('elasticsearch')
const client = new Client({
  node: 'http://localhost:9200'
})

// ping the client to be sure Elasticsearch is up
const countries = require('./countries.json');
// declare an empty array called bulk
var bulk = [];
//loop through each city and create and push two objects into the array in each loop
//first object sends the index and type you will be saving the data as
//second object is the data you want to index


async function run () {
  await client.indices.create({
    index: 'countries-data',
    body: {
      mappings: {
        properties: {
          name: { type: 'text' },
          code: { type: 'text' },
        }
      }
    }
  }, { ignore: [400] });

  //create data 
  countries.forEach(country =>{
    bulk.push(country)
  });
  const body = bulk.flatMap(doc => [{ index: { _index: 'countries-data' } }, doc])

  const { body: bulkResponse } = await client.bulk({ refresh: true, body })


  if (bulkResponse.errors) {
    const erroredDocuments = []
    // The items array has the same order of the dataset we just indexed.
    // The presence of the `error` key indicates that the operation
    // that we did for the document has failed.
    bulkResponse.items.forEach((action, i) => {
      const operation = Object.keys(action)[0]
      if (action[operation].error) {
        erroredDocuments.push({
          // If the status is 429 it means that you can retry the document,
          // otherwise it's very likely a mapping error, and you should
          // fix the document before to try it again.
          status: action[operation].status,
          error: action[operation].error,
          operation: body[i * 2],
          document: body[i * 2 + 1]
        })
      }
    })
    console.log(erroredDocuments)
  }

  const { body: count } = await client.count({ index: 'countries-data' })
  console.log(count)


}

run().catch(console.log)



//perform bulk indexing of the data passed
// client.bulk({body:bulk}, function( err, response  ){ 
//          if( err ){ 
//              console.log("Failed Bulk operation".red, err) 
//          } else { 
//              console.log("Successfully imported %s".green, countries.length); 
//          } 
// }); 
