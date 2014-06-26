// movedata_queue

// documentation via: haraka -c /home/jkentjnr/Documents/movedata/haraka -h plugins/movedata_queue

// Put your plugin code here
// type: `haraka -h Plugins` for documentation on how to create a plugin

var fs = require('fs')
  , sys = require('sys')
  , request = require('request')
  , Buffers = require('buffers')
  , _ = require('underscore')
  , headers = {'content-type':'application/json', 'accept':'application/json'}
  , transactions = {}
  ;

var MC = require('mongomq').MongoConnection 
  , MQ = require('mongomq').MongoMQ
  , mq = null;

exports.register = function () {
	var options = {
		databaseName: this.config.get('mongomq.database') || 'tests', 
		queueCollection: this.config.get('mongomq.collection') || 'capped_collection', 
		autoStart: false
	};

	mq = new MQ(options);

	(function(){
	  var logger = new MC(options);
	  logger.open(function(err, mc){
	    if(err){
	      console.log('ERROR: ', err);
	    }else{
	      mc.collection('envelope', function(err, loggingCollection){
	        loggingCollection.remove({},  function(){
	          mq.start(function(err){
	            //putRecord();
	          });
	        });
	      });
	    }
	  });
	})();
};


exports.hook_data = function (next, connection) {
    // enable mail body parsing
    connection.transaction.parse_body = 1;
    connection.transaction.attachment_hooks(
        function (ct, fn, body, stream) {
            start_att(connection, ct, fn, body, stream)
        }
    );
    next();
}

function start_att (connection, ct, fn, body, stream) {
    //connection.loginfo("Got attachment: " + ct + ", " + fn + " for user id: " + connection.transaction.notes.hubdoc_user.email);
    //connection.transaction.notes.attachment_count++;

	//debugger;

	if (typeof connection.transaction.notes.attachments == 'undefined')
		connection.transaction.notes.attachments = [];

    stream.connection = connection; // Allow backpressure
    //stream.pause();

	var bufs = [];
	stream
		.on('data', function(d){ 
			bufs.push(d); 
		})
		.on('end', function() { 
			connection.transaction.notes.attachments.push({ 
				filename: fn, 
				content_type: ct.replace(/\n/g, " "), 
				data: Buffer.concat(bufs).toString('base64') 
			}); 

			connection.logdebug('DATA: ' + Buffer.concat(bufs).toString('utf-8'));
		});
}

function extractChildren(children) {
  return children.map(function(child) {
    var data = {
      bodytext: child.bodytext,
      headers: child.header.headers_decoded
    }
    if (child.children.length > 0) data.children = extractChildren(child.children);
    return data;
  }) 
}

function parseSubaddress(user) {
  var parsed = {username: user};
  if (user.indexOf('+')) {
    parsed.username = user.split('+')[0];
    parsed.subaddress = user.split('+')[1];
  }
  return parsed;
}

exports.hook_queue = function(next, connection) {
  var common = {} //transactions[connection.transaction.uuid].doc()
    , body = connection.transaction.body
    , docCounter = 0
    , baseURI = this.couchURL + "/" + this.dbPrefix
    ;

  connection.logdebug(JSON.stringify(connection.transaction.rcpt_to));
  common['headers'] = body.header.headers_decoded;
  common['bodytext'] = body.bodytext;
  common['content_type'] = body.ct;
  common['parts'] = extractChildren(body.children);

  common['attachments'] = (typeof connection.transaction.notes.attachments != 'undefined') ? connection.transaction.notes.attachments : null;

  var dbs = connection.transaction.rcpt_to.map(function(recipient) {
    docCounter++;
    var db = {doc: {tags: []}};
    var user = parseSubaddress(recipient.user);
    db.uri = baseURI + user.username;
    db.doc.recipient = recipient;
    if (user.subaddress) db.doc.tags.push(user.subaddress);
    db.doc = _.extend({}, db.doc, common);
    return db;
  })
  
  function resolve(err, resp, body) {
    docCounter--;
    if (docCounter === 0) {
      next(OK); 
    }
  }

  dbs.map(function(db) {
    var message = {uri: db.uri, method: "POST", headers: headers, body: JSON.stringify(db.doc)};
    mq.emit('envelope', message);
    resolve();
  });
};


/*
var fs = require('fs')
  , Buffers = require('buffers')
  , transactions = {}
  ;

function attachment() {
  return function() {
    var bufs = Buffers()
      , doc = {_attachments: {}}
      , filename
      ;
    return {
      start: function(content_type, name) {
        filename = name;
        doc._attachments[filename] = {content_type: content_type.replace(/\n/g, " ")};
      },
      data: function(data) { bufs.push(data) },
      end: function() { if(filename) doc._attachments[filename]['data'] = bufs.slice().toString('base64') },
      doc: function() { return doc }
    }
  }();
}

function extractChildren(children) {
  return children.map(function(child) {
    var data = {
      bodytext: child.bodytext,
      headers: child.header.headers_decoded
    }
    if (child.children.length > 0) data.children = extractChildren(child.children);
    return data;
  }) 
}

exports.hook_data = function (next, connection) {
  connection.transaction.parse_body = true;
  var attach = transactions[connection.transaction.uuid] = attachment();
  connection.transaction.attachment_hooks(attach.start, attach.data, attach.end);
  next();
}

exports.hook_queue = function(next, connection) {
debugger;
  var common = transactions[connection.transaction.uuid].doc()
    , body = connection.transaction.body
    , docCounter = 0
    , baseURI = this.couchURL + "/" + this.dbPrefix
    ;

  connection.logdebug(JSON.stringify(connection.transaction.rcpt_to));
  common['headers'] = body.header.headers_decoded;
  common['bodytext'] = body.bodytext;
  common['content_type'] = body.ct;
  common['parts'] = extractChildren(body.children);
    
};
*/