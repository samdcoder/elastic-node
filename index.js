//index.js
//require the Elasticsearch librray

const { Client } = require('@elastic/elasticsearch')
const client = new Client({ node: 'http://localhost:9200' })
// instantiate an elasticsearch client
// const client = new Client({
//    hosts: [ 'http://localhost:9200']
// });
//require Express
const express = require( 'express' );
// instanciate an instance of express and hold the value in a constant called app
const app     = express();
//require the body-parser library. will be used for parsing body requests
const bodyParser = require('body-parser')
//require the path library
const path    = require( 'path' );

const {indexName} = require('./config.json');

// ping the client to be sure Elasticsearch is up
client.ping({
     requestTimeout: 30000,
 }, function(error) {
 // at this point, eastic search is down, please check your Elasticsearch service
     if (error) {
         console.error('elasticsearch cluster is down!');
     } else {
         console.log('Everything is ok');
     }
 });


// use the bodyparser as a middleware  
app.use(bodyParser.json())
// set port for the app to listen on
app.set( 'port', process.env.PORT || 3001 );
// set path to serve static files
app.use( express.static( path.join( __dirname, 'public' )));
// enable CORS 
app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header('Access-Control-Allow-Methods', 'PUT, GET, POST, DELETE, OPTIONS');
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});

// defined the base route and return with an HTML file called tempate.html
app.get('/', function(req, res){
  res.sendFile('template.html', {
     root: path.join( __dirname, 'views' )
   });
})

// define the /search route that should return elastic search results 
app.get('/search', async function (req, res){
  const given = req.query.q;
  console.log('given => ', given)
  console.log('indexName => ', indexName)
  const { body } = await client.search({
    index: indexName,
    body: {
      query: {
        match: {
          city: given
        }
      }
    }
  })

 
  console.log('result => ', body)
  res.send(body.hits.hits);

})
// listen on the specified port
app .listen( app.get( 'port' ), function(){
  console.log( 'Express server listening on port ' + app.get( 'port' ));
} );