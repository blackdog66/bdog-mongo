package bdog.mongo;

import haxe.rtti.CType;
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
  function remove(query:MongoObj):Void;
  function find(?query:MongoQuery,?meta:MongoMeta,fn:MongoErr->Cursor<T>->Void):Void;
  function findOne(?query:MongoQuery,fn:MongoErr->T->Void):Void;
  function drop(fn:MongoErr->Collection<T>):Void;
  function update(q:MongoQuery,d:T,options:Dynamic,fn:MongoErr->T->Void):Void;
}

interface Database {
  function open(fn:MongoErr->Database->Void):Void;
  function dropDatabase(fn:MongoErr->Dynamic->Void):Void;
  function close():Void;
  function collection<T>(name:String,fn:MongoErr->Collection<T>->Void):Void;
  function createCollection<T>(name:String,fn:MongoErr->Collection<T>->Void):Void;
  function getStatus():String;
}

class MongoPool {
  public static var mongo_db:Dynamic;
  
  var host:String;
  var port:Int;
  var name:String;
  var size:Int;

  var connections:Array<Database>;
  
  public function new(host:String,port:Int,name:String,size:Int) {
    if (mongo_db == null)
      mongo_db = Node.require("mongodb");

    this.host = host;
    this.port = port;
    this.name = name;
    
    connections = new Array();
    for (i in 0...size)
      connections.push(DB(host,port,name));

    trace("Pool set to  "+connections.length);
  }
  
  public function
  connection():Database {
    if (connections.length > 0) {
      return connections.pop();
    } else {
      return DB(host,port,name);
    }
  }

  public function
  returnConnection(c:Database) {
    c.close();
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
  static var rttis = new Hash<haxe.rtti.TypeTree>();
  
  static function
  getRTTI(c:Dynamic):haxe.rtti.TypeTree {
    var
      cn = Type.getClassName(c),
      rt = rttis.get(cn);

    if (rt == null) {
      var
        rtti : String = untyped c.__rtti;      
      if (rtti == null) throw "NO RTTI! for "+cn;
      rt = new haxe.rtti.XmlParser().processElement(Xml.parse(rtti).firstElement());
      rttis.set(cn,rt);
    }
    
    return rt;
  }
  
  public function new(mdb:MongoDB,c:Class<T>) {
    var n = Type.getClassName(c).split(".");
    name = n[n.length-1];
    this.mdb = mdb;
    myclass = c;    
  }

  function serialize(o:T):Dynamic {
    var final:Dynamic = null;
    switch(Type.typeof(o)) {
    case TNull: final = null;
    case TClass(kls):
      var z = {};
      for (f in Type.getInstanceFields(kls)) {
        var val:Dynamic = Reflect.field(o,f);
        if (val != null && !Reflect.isFunction(val)) {
          Reflect.setField(z,f,switch(Type.typeof(val)) {
            case TInt, TBool, TFloat:
              val;
            case TClass( c ):
              var cn = Type.getClassName(c);
              if (cn == "Array") {
                var na = new Array<Dynamic>();
                for (el in cast(val,Array<Dynamic>)) {
                  na.push(serialize(el));
                }
                na;
              } else {
                if (cn != "String")
                  serialize(val);
                else
                  val;
              }
            case TEnum(_):
              Type.enumConstructor(val);          
            default:
              val;
            });
        }
      }
      final = z;
    case TEnum(e):
      final = Type.enumConstructor(o);
    default:
      if (!Reflect.isFunction(o))
        final = o;
    }
    return final;
  }

  function deserClass(o,klsPath:String) {
    var
      me = this,
      resolved = Type.resolveClass(klsPath),
      newObj = Type.createEmptyInstance(resolved),
      mid = MongoDB.ID(o);

    if (mid != null) Reflect.setField(newObj,"_id",mid);
    
    switch(getRTTI(resolved)) {
    case TClassdecl(typeInfo):
      Lambda.iter(typeInfo.fields,function(el) {
          var val = Reflect.field(o,el.name);
          me.classFld(newObj,el.name,val,el.type);
        });
    default:
    }
    return newObj;
  }
  
  function classFld(newObj:Dynamic,name:String,val:Dynamic,el:CType){
    var me = this;
    switch(el) {
    case CClass(kls,subtype):
      switch(kls) {
      case "String","Float","Int":

        Reflect.setField(newObj,name,val);
      
      case "Array":
        var
          na = new Array<Dynamic>(),
          st = subtype.first();
        for (i in cast(val,Array<Dynamic>)) {
          switch(st) {
          case CClass(path,_):
            na.push(deserClass(i,path));
          case CEnum(enumPath,_):
            var e = Type.resolveEnum(enumPath);
            na.push(Type.createEnum(e,i));
          default:
            na.push(i);
          }
        }
        
        Reflect.setField(newObj,name,na);

      default:
        Reflect.setField(newObj,name,deserClass(val,kls));
      }
      
    case CEnum(enumPath,_):
      var e = Type.resolveEnum(enumPath);
      Reflect.setField(newObj,name,Type.createEnum(e,val));
      
    default:
      trace("other deser type"+el);
    }
  }  
  
  public function
  deserialize(o,mykls:Class<Dynamic>):T {    
    if (o == null) return null;
    return deserClass(o,Type.getClassName(mykls));
  }

  public function
  withCol(fn) {
    var me = this;
    if (mdb.getStatus() == "notConnected") {
      mdb.open(function(err,db) {
          db.collection(me.name,function(err,coll) {
              if (err != null) trace("prob with coll:"+me.name);
              fn(coll);
            });
        });
    } else {
      mdb.collection(me.name,function(err,coll) {
          if (err != null) trace("prob with coll:"+me.name);
          fn(coll);
        });
    }
  }

  public function
  insert(o:T,?fin:T->Void) {
    var me = this;
    withCol(function(col) {
        var s:Dynamic = me.serialize(o);
        col.insert(s, function(err,n:T) {
            if (err != null)
              trace(err);
            
            if (fin != null) {
              var v = me.deserialize(n,me.myclass);
              trace("deser kls:"+Type.getClassName(Type.getClass(v)));
              fin(v);
            }
          });
      });
  }

  public function
  insertMany(o:Iterable<T>,?fin:Dynamic->Void) {
    var me = this;
    withCol(function(col) {
        var d:Dynamic = Lambda.array(Lambda.map(o,me.serialize)); 
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
            fn(me.deserialize(doc,me.myclass));
          });
      });
  }

  public function
  update(q:MongoQuery,d:T,?options:MongoUpdate,?fn:Dynamic->Void) {
    withCol(function(col) {
        col.update(q,d,options,function(err,doc) {
            if (fn != null)
              fn(doc);
          });
      });
  }
}

class MongoDB implements Database {
  var pool:MongoPool;
  var db:Database;
  
  public function new(p:MongoPool) {
    pool = p;
    db = pool.connection();
  }

  public static function
  parse(d:Dynamic) {
    return Node.parse(StringTools.replace(Node.stringify(d),"_","$"));
  }

  public static function
  ID(o:Dynamic) {
    return Reflect.field(o,"_id");
  }
  
  public function col<T>(c:Class<T>) {
    return new EasyCol<T>(this,c);
  }
  
  public function open(fn:MongoErr->Database->Void) {}
  public function dropDatabase(fn:MongoErr->Dynamic->Void) {}
  public function close() {}
  public function collection<T>(name:String,fn:MongoErr->Collection<T>->Void) {}
  public function createCollection<T>(name:String,fn:MongoErr->Collection<T>->Void) {}
  public function connection() {}

  public function getStatus() {
    return untyped db.state;
  }
 }


