package bdog.mongo;

// implement ChrisKvs work

import bdog.Os;
import js.Node;
import bdog.mongo.Mongo;

class NativeCollection<T> implements Collection<T> {
  var col:Dynamic;
  
  public function new(col:Collection<T>) {
    this.col = col;
  }
  
  public function
  insert(rec:T,fn:MongoErr->T->Void) {
    col.insert(rec,function(err,a) {
        fn(err,a[0]);
      });
  }

  public function
  insertMany(recs:Array<T>,fn:MongoErr->Array<T>->Void) {
    col.insert(recs,fn);
  }

  public inline function
  count(fn:MongoErr->Int->Void) {
    col.count(fn);
  }
  
  public function
  findOne(?query:MongoQuery,fn:MongoErr->T->Void) {
    col.findOne(MongoDB.parse(query),fn);
  }

  public inline function
  find(?query:MongoQuery,?sort:MongoMeta,fn:MongoErr->Cursor<T>->Void) {
    col.find(MongoDB.parse(query),sort,fn);
  }

  public inline function
  remove(query:MongoObj) {
    col.remove(query);
  }

  public inline function
  drop(fn:MongoErr->Collection<T>) {
    col.drop(fn);
  }

  public inline function
  update(query:MongoQuery,d:T,options:Dynamic,fn:MongoErr->T->Void) {
    col.update(query,d,options,fn);
  }

}

class MongoNative extends MongoDB {
  
  public function new(p:MongoPool) {
    super(p);
    if (db == null) trace("pool exhausted");
  }

  override function
  open(fn:MongoErr->Database->Void) {
    var me = this;
    db.open(function(err,db) {
        fn(err,me);
      });
  }
  
  override function
  dropDatabase(fn:MongoErr->Dynamic->Void) {
    db.dropDatabase(fn);
  }
  
  override function
  close() {
    pool.returnConnection(db);
  }
  
  override function
  collection<T>(name:String,fn:MongoErr->Collection<T>->Void) {
    db.collection(name,function(err,col) {
        fn(err,new NativeCollection(col));
      });
  }

  override function
  createCollection<T>(name:String,fn:MongoErr->Collection<T>->Void) {
    db.createCollection(name,function(err,col) {
        fn(err,new NativeCollection(col));
      });
  }
     
}

