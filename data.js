'use strict'

//this is a one time script used to populate elastic search DB with countries data which is stored is countries.json

require('array.prototype.flatmap').shim()
const { Client } = require('elasticsearch')
const client = new Client({
  node: 'http://localhost:9200'
})

// ping the client to be sure Elasticsearch is up
const countries = require('./cities.json');
const {indexName} = require('./config.json');
// declare an empty array called bulk
var bulk = [];
//loop through each city and create and push two objects into the array in each loop
//first object sends the index and type you will be saving the data as
//second object is the data you want to index


async function run () {
  await client.indices.create({
    index: indexName,
    body: {
      mappings: {
        properties: {
          country: { type: 'text' },
          city: { type: 'text' },
          lat: { type: 'text' },
          lng: { type: 'text' },

        }
      }
    }
  }, { ignore: [400] });
  console.log('length => ', countries.length);

  //create data 
  // for(var i = 0; i < countries.length; i++){
  //   const {country, name:city, lat, lng} = countries[i];
  //   const inputObject = {
  //     country,
  //     city, 
  //     lat, 
  //     lng
  //   }
  //   console.log('inputObject => ',  inputObject);
  //   bulk.push(inputObject)
  // }
  countries.forEach(c =>{

    const {country, name:city, lat, lng} = c;
    const inputObject = {
      country,
      city, 
      lat, 
      lng
    }
    bulk.push(inputObject)
  });
  console.log("bulk length => ", bulk.length)

  const body = bulk.flatMap(doc => [{ index: { _index: indexName } }, doc])

  const { body: bulkResponse } = await client.bulk({ refresh: true, body })


  if (bulkResponse && bulkResponse.errors) {
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

  const { body: count } = await client.count({ index: indexName })
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
