
import js.Node;
import bdog.mongo.Mongo;
import bdog.mongo.MongoNative;

typedef Woot = {
  var a:String;
}

class Test {
  static var pool:MongoPool;

  public inline static function
  DB() {
    return new MongoNative(pool);
  }
  
  public static function
  main() {
    pool = new MongoPool('localhost',27017,'ritchie',3);    
    easy();    
  }

  public static function
  easy() {
    var db = DB();
    db.col('newish').insert({counter:1,hello:'there'});
    for (i in 0...10)
      db.col('newish').insert({counter:i,hello:'rhere'+i});

    db.col('newish').count(function(c) {
        trace('have now '+c);
      });
  }

  public static function
  primitives() {

    for(i in 0...200) {
   
      new MongoNative(pool).open(function(err,db) {
          if (err != null) trace(err);
         trace("opening "+i);

        db.collection('woot',function(err,coll) {
            coll.find({a:{_lte:3}},function(err,cursor) {
                cursor.each(function(err,woot:Woot) {
                    //         trace(woot);
                  });
              });
          });
        
        db.collection('woot',function(err,coll) {
            coll.insert({nice:'one laddie'},function(err,doc) {
                 if (err != null) trace(err);
                //trace(doc);
                 coll.count(function(err,c) {
                     trace("n recs is "+c);
                     db.close();
                   });
               
                
              });
          });

          db.createCollection('stuff',function(err,coll) {
            coll.insert({mystuff:[1,2,3]},function(err,doc) {
                db.close();
              });
          });
        
      });
    }
  }
  
}