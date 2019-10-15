//require the Elasticsearch librray
const elasticsearch = require('elasticsearch');
// instantiate an Elasticsearch client
const client = new elasticsearch.Client({
   hosts: [ 'http://localhost:9200']
});
// ping the client to be sure Elasticsearch is up
const countries = require('./countries.json');
// declare an empty array called bulk
var bulk = [];
//loop through each city and create and push two objects into the array in each loop
//first object sends the index and type you will be saving the data as
//second object is the data you want to index
console.log('countries => ', countries);
countries.forEach(country =>{
   bulk.push({index:{ 
                 _index:"scotch.io-tutorial", 
                 _type:"countries_list",
             }          
         })
  bulk.push(country)
})
//perform bulk indexing of the data passed
client.bulk({body:bulk}, function( err, response  ){ 
         if( err ){ 
             console.log("Failed Bulk operation".red, err) 
         } else { 
             console.log("Successfully imported %s".green, countries.length); 
         } 
}); 
