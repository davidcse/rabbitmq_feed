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
            return res.json({"msg": msg.content.toString()});
          }, {noAck: true});
        });
    });
    setTimeout(function() {
      conn.close();
      return res.json({"msg":"timedout error"});
    }, 10000);
    console.log("Finished /listen");
  });


});


app.post('/speak',function(req,res){
  if(!req.body.key || !req.body.msg){
    return res.json({"status":"error"});
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
      return res.json({"status":"OK"});
    });
    setTimeout(function() { conn.close(); return res.json({"status":"timedout"})}, 500);
  });
  console.log("Finished /speak");
});



// Starts the server
var server = app.listen(80, function () {
  var host = server.address().address
  var port = server.address().port
  console.log("RabbitMQ server is listening at http://%s:%s", host, port)
})
