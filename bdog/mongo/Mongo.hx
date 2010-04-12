package bdog.mongo;

import haxe.rtti.CType;
import bdog.Serialize;
import js.Node;
using Lambda;


typedef MongoErr = Dynamic;
typedef MongoObj = Dynamic;
typedef MongoQuery=Dynamic;
typedef MongoUpdate=Dynamic;

typedef DBMeta = {
  var colName:String;
  var fld:String;
  var kls:String;
}

typedef MongoMeta = {
  var skip:Int;
  var limit:Int;
  var sort:String;
}

typedef Server = {
  var host:String;
  var port:Int;
  var options:Dynamic;
  var internalMaster:Bool;
  var autoReconnect:Bool;
}
  
typedef Cursor<T> = {
  function each(fn:MongoErr->T->Void):Void;
  function nextObject(fn:MongoErr->T):Void;
  function toArray():Array<T>;
}

interface Collection<T>  {
  function insert(rec:T,fn:MongoErr->T->Void):Void;
  function insertMany(recs:Array<T>,fn:MongoErr->Array<T>->Void):Void;
  function count(fn:MongoErr->Int->Void):Void;
  function remove(query:MongoObj,?options:Dynamic):Void;
  function find(?query:MongoQuery,?meta:MongoMeta,fn:MongoErr->Cursor<T>->Void):Void;
  function findOne(?query:MongoQuery,fn:MongoErr->T->Void):Void;
  function drop(fn:MongoErr->Collection<T>):Void;
  function update(q:MongoQuery,d:Dynamic,options:Dynamic,fn:MongoErr->T->Void):Void;
  
}

interface Database {
  function open(fn:MongoErr->Database->Void):Void;
  function dropDatabase(fn:MongoErr->Dynamic->Void):Void;
  function close():Void;
  function collection<T>(name:String,fn:MongoErr->Collection<T>->Void):Void;
  function createCollection<T>(name:String,fn:MongoErr->Collection<T>->Void):Void;
  function getStatus():String;
  function lastStatus(fn:Dynamic->Dynamic->Void):Void;
}

class MongoPool {
  public static var mongo_db:Dynamic;
  
  var host:String;
  var port:Int;
  var name:String;
  var size:Int;

  var connections:Array<Database>;
  public var ready:Void->Void;
  static public var ObjectID:Dynamic; // is a class
  
  public function new(host:String,port:Int,name:String,size:Int) {
    if (mongo_db == null)
      mongo_db = Node.require("mongodb");

    if (ObjectID == null) {
      var bson = Node.require("mongodb/bson/bson");
      ObjectID = bson.ObjectID;
    }
    
    this.host = host;
    this.port = port;
    this.name = name;
    var me = this;
    connections = new Array();
    for (i in 0...size) {
      addConnection(function() {
          if (me.connections.length == size) {
            trace("Pool set to  "+me.connections.length);
            me.ready();
          }
        });
    }
  }

  function addConnection(?onAddition:Void->Void) {
    var db = DB(host,port,name);
    var me = this;
    db.open(function(err,db) {
        me.connections.push(db);
        if (onAddition != null)
          onAddition();
      });
  }

  public function
  connection():Database {
    if (connections.length > 0) {
      return connections.pop();
    } else {
      addConnection();
      return connections.pop();
    }
  }

  public function
  returnConnection(c:Database) {
    //c.close();
    connections.push(c);
    trace("pool size = "+connections.length);
  }

  static function
  server(host,port):Server {
    return untyped __js__("new bdog.mongo.MongoPool.mongo_db.Server(host,port,{});");
  }
  
  static function
  DB(host,port,name):Database {
    var server = server(host,port);
    return untyped __js__("new bdog.mongo.MongoPool.mongo_db.Db(name,server,{});");
  }

}

private class EasyCol<T> {
  var name:String;
  var mdb:MongoDB;
  var myclass:Class<T>;
    
  public function new(mdb:MongoDB,c:Class<T>) {
    var n = Type.getClassName(c).split(".");
    name = n[n.length-1];
    this.mdb = mdb;
    myclass = c;    
  }

  public function
  withCol(fn) {
    mdb.collection(name,function(err,coll) {
        if (err != null) trace(err);
        fn(coll);
      });
  }

  public function
  insert(o:T,?fin:T->Void) {
    var me = this;
    withCol(function(col) {
        var s:Dynamic = Serialize.classToDoc(o);
        col.insert(s, function(err,n:T) {
            if (err != null)
              trace(err);            
            if (fin != null) {
              fin(Serialize.docToClass(n,me.myclass));
            }
          });
      });
  }

  public function
  insertMany(o:Iterable<T>,?fin:Dynamic->Void) {
    var me = this;
    withCol(function(col) {
        var d:Dynamic = Lambda.array(Lambda.map(o,Serialize.classToDoc)); 
        col.insertMany(d, function(err,n) {
            if (fin != null)
              fin(n);
          });
      });
  }

  public function
  count(fin:Int->Void) {
    withCol(function(col) {
        col.count(function(err,c) {
            fin(c);
          });        
      });
  }

  public function
  find(q:MongoQuery,?meta:MongoMeta,fn:Cursor<T>->Void){
    withCol(function(col) {
        col.find(q,meta,function(err,cursor) {
            fn(cursor);
          });
      });
  }

  public function
  findOne(q:MongoQuery,fn:T->Void){
    var me = this;
    withCol(function(col) {
        col.findOne(q,function(err,doc) {
            fn(Serialize.docToClass(doc,me.myclass));
          });
      });
  }

  public function
  update(q:MongoQuery,d:Dynamic,options:MongoUpdate,?extfn:Dynamic->Void) {
    withCol(function(col) {
        col.update(q,d,options,function(err,doc) {
            if (err != null)
              throw "problem updating "+err+"\n>> "+d+"\n>>"+Node.sys.inspect(q);
            else {
              if (extfn != null) {
                extfn(doc);
              }
            }
          });
      });
  }
  

  public function
  remove(q:MongoQuery,?options:Dynamic) {
    withCol(function(col) {
        col.remove(MongoDB.parse(q),options);
      });
  }
}

class MongoDB implements Database {
  var pool:MongoPool;
  var db:Database;
  
  public function new(p:MongoPool) {
    pool = p;
    db = pool.connection();
    if (db == null) trace("pool exhausted");
  }

  public static inline function
  ID(o:Dynamic) {
    return Reflect.field(o,"_id");
  }

  static function
  nativeID(d:Dynamic,fn:Dynamic->Dynamic) {

    var id = ID(d);

    if (id != null){
      var Obj = MongoPool.ObjectID;
      id = untyped __js__("new Obj(id)");
      Reflect.deleteField(d,"_id");
    }

    var s = fn(d);
        
    if (id != null)
      Reflect.setField(s,"_id",id);

    return s;
  }
  
  public static function
  parse(d:Dynamic) {    
    return nativeID(d,function(z) {
        return z;
      });
  }
    
  public function col<T>(c:Class<T>) {
    return new EasyCol<T>(this,c);
  }

  public function close() {
    pool.returnConnection(db);
  }
  
  public function getStatus() {
    return untyped db.state;
  }

  public function lastStatus(fn:Dynamic->Dynamic->Void) {}  
  public function open(fn:MongoErr->Database->Void) {}
  public function dropDatabase(fn:MongoErr->Dynamic->Void) {}
  public function collection<T>(name:String,fn:MongoErr->Collection<T>->Void) {}
  public function createCollection<T>(name:String,fn:MongoErr->Collection<T>->Void) {}
  public function connection() {}

 }


