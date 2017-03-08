/*
  Server program for rabbitmq listen/speak exchange
*/

//Instantiate libraries
var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var amqp = require('amqplib/callback_api');
//APP CONFIG
app.use(bodyParser.urlencoded({extended : true}));
app.use(bodyParser.json());
//variables
var ex = 'hw3';


/************************************/
/*  REST ENDPOINTS                  */
/***********************************/
app.post('/listen', function (req, res) {
  console.log("POST /listen : " + JSON.stringify(req.body));
  var keys = [];
  if(!req.body.keys || ! typeof(req.body.keys) =="object"){
    return
  }
  keys = req.body.keys;
  amqp.connect('amqp://localhost', function(err, conn) {
    conn.createChannel(function(err, ch) {
      ch.assertExchange(ex, 'direct', {durable: false});
        ch.assertQueue('', {exclusive: true}, function(err, q) {
          console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", q.queue);
          for(var i=0;i<keys.length;i++){
            ch.bindQueue(q.queue, ex, keys[i]);
          }
          ch.consume(q.queue, function(msg) {
            console.log(" Success Consuming: [x] %s", msg.content.toString());
            return res.json({"msg":msg.content.toString()});
          }, {noAck: true});

        });
    });
    //setTimeout(function() { conn.close(); }, 500);
    console.log("Finished /listen");
  });


});


app.post('/speak',function(req,res){
  if(!req.body.key || !req.body.msg){
    return
  }
  console.log("POST /speak : " + JSON.stringify(req.body));
  //publish to the hw3 Exchange
  amqp.connect('amqp://localhost', function(err, conn) {
    conn.createChannel(function(err, ch) {
      ch.assertExchange(ex, 'direct', {durable: false});
      // extract var
      var key = req.body.key;
      var msg = req.body.msg;
      //publish msg to exchange, using key.
      ch.publish(ex, key, Buffer.from(msg));
      console.log(" [x] Sent msg:%s    key: %s", msg, key);
      res.json({"status":"OK"});
    });
    // setTimeout(function() { conn.close()}, 500);
  });
  console.log("Finished /speak");
});


/*

app.get('/speak',function(req,res){
  console.log("speak works");
  console.log("_______________________________________________");
  var amqp = require('amqplib/callback_api');

  amqp.connect('amqp://localhost', function(err, conn) {
    conn.createChannel(function(err, ch) {
      var ex = 'logs2';
      var msg = 'Hello World!';
      ch.assertExchange(ex, 'direct', {durable: false});
      ch.publish(ex, "mr.muffin", new Buffer(msg));
      console.log(" [x] Published to exchange: %s , message: %s", ex,msg);
      res.json({"status":"OK","message":"Finished Producing"});
    });
    console.log("Timeout!!!!!!!!\n");
    setTimeout(function() { conn.close();}, 500);
  });
});


app.get('/listen',function(req,res){
  console.log("listen works");
  console.log("_______________________________________________");
  var amqp = require('amqplib/callback_api');
  var ex = 'logs2';

  amqp.connect('amqp://localhost', function(err, conn) {
    conn.createChannel(function(err, ch) {
      ch.assertExchange(ex, 'direct', {durable: false});
      try{
        ch.assertQueue('myqueue', {exclusive: true}, function(err, q) {
          if(err){
            console.log("first try block error: "+err);
          }
          console.log('Asserted Queue : %s',q.queue);
          ch.bindQueue(q.queue, ex, 'mr.muffin');
          ch.consume(q.queue, function(msg) {
            console.log(" [x] Finished Consuming string : %s", msg.content.toString());
            res.json({"status":"OK","message":"Finished Consuming"});
          }, {noAck: true});
          res.json({"status":"error took too long"});
        });
      }catch(err){
        console.log("Caught an ERROR when asserting the queue");
        try{
          ch.consume('myqueue', function(msg) {
            console.log(" [x] Finished Consuming string : %s", msg.content.toString());
            res.json({"status":"OK","message":"Finished Consuming"});
          }, {noAck: true});
          res.json({"status":"error took too long"});
        }catch(err2){
          console.log("caught ERROR2");

        }

      }
    });
  });
});


*/


// Starts the server
var server = app.listen(80, function () {
  var host = server.address().address
  var port = server.address().port
  console.log("RabbitMQ server is listening at http://%s:%s", host, port)
})
