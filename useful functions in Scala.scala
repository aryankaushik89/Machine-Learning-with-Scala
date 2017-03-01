import org.apache.log4j.Logger
import org.apache.log4j.Level
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import sqlContext.implicits._
import org.apache.spark.sql._

// Run any HiveQL command using sqlContext
sqlContext.sql("use twitter");
// Creating a DataFrame pointing to my Hive table 
val f = sqlContext.table("twitter.full_text_ts")

// Get the schema of the DataFrame
f.printSchema

// Column projection 
val f_proj = f.select($"id", $"ts")

// Show first 20 records
f_proj.show

// above can also be written as:
f.select($"id", $"ts").show

// limit the number of records to show
f.select($"id", $"ts").show(5)


// chaining function calls
f.select($"id", $"ts").where($"ts" >= "2010-03-04").show(5)

// may also use 'filter' instead of 'where'
f.select($"id", $"ts").filter($"ts" >= "2010-03-04").show(5)

// filter with multiple conditions
f.filter($"ts" > "2010-03-03 01" && $"ts" < "2010-03-03 02:00").show

// note three equal signs - equality check for Column
f.filter($"id" === "USER_afa7ebf8").show(5)

// get the limited number of records, equivalent to limit in SQL
val lf = f.select($"id", $"ts").take(10)
f.select($"id", $"ts").take(10).foreach(println)

// Converting array of Row back to DataFrame
val lf_schema = f.select($"id", $"ts").schema
val lf_df = sqlContext.createDataFrame(sc.parallelize(lf), lf_schema)

// Aggregate operations: GroupBy
f.groupBy($"id").count.show(5)
f.groupBy($"id").count.orderBy($"count".desc).show(5)

f.groupBy($"id").agg(avg($"lat").alias("avg_lat")).show(5)
f.groupBy($"id").agg(avg($"lat").alias("avg_lat"), max($"lon").alias("max_lon")).show(5)

// Casting data types
import org.apache.spark.sql.types._
val fcl = f.select($"lat".cast(DoubleType).alias("latitude"))
val fcl = f.select($"lat".alias("lat") cast DoubleType)	// alternate syntax

val fc = 
    f.select($"id", $"ts", $"lat".cast(DoubleType).alias("latitude"), 
    $"lon".cast(DoubleType).alias("longitude"), $"tweet")

// joining DataFrames
/*
here is the script to create a table if not defined in hive 

create table twitter.dayofweek (date_field string, dayofweek string)
row format delimited
fields terminated by '\t';

load data inpath '/user/root/lab/twitter/dayofweek.txt'
overwrite into table twitter.dayofweek;
*/

val g = sqlContext.table("twitter.dayofweek")
val fc_wd = fc.withColumn("ts_date", $"ts".cast(StringType).substr(1,10))
val j = fc_wd.join(g, fc_wd("ts_date") === g("date_field"))
val jp = j.select($"id", $"ts_date", $"dayofweek", $"latitude", $"longitude")


// write data back to hive
jp.write.saveAsTable("twitter.full_text_dow")

// may specify the format as well
jp.write.format("parquet").saveAsTable("twitter.full_text_dow")

// load the saved data again through hive metastore
val ft_dow = sqlContext.table("twitter.full_text_dow")

// Starting Spark 1.5, may specify the database name 
jp.write.saveAsTable("twitter.full_text_dow")


//// map over records
Seq(1, 2, 3).map(x => x*5)
Seq(1, 2, 3).map(_*5)	// equivalent to the above


////// Functions
def sqNum(x:Double): Double = x*x
sqNum(5)

// Get the rdd of double values
fc.select($"latitude").limit(5).map(v => (v(0).asInstanceOf[Double]))

// Now map through double rdd and apply the function
fc.select($"latitude").limit(5).map(v => (v(0).asInstanceOf[Double])).
     map(u => sqNum(u)).foreach(println)

// Alternatively read directly from Row: get the value in first column and cast to string
fc.select($"latitude").limit(5).map(v => sqNum(v(0).asInstanceOf[Double])).foreach(println)

// or use the following that doesn't require type-casting
fc.select($"latitude").limit(5).map{case Row(x:Double) => sqNum(x)}.foreach(println)

// even better, replace map with foreach
fc.select($"latitude").take(5).foreach{case Row(x:Double) => println(sqNum(x))}

// Similarly create add function that takes two parameters
def myAdd(x: Double, y: Double): Double = x+y
myAdd(3.4, 5.3)
myAdd(3.4, 5)

// maping through the rows and add latitude and longitude
fc.select($"latitude", $"longitude").limit(5).map(v => myAdd(v(0).asInstanceOf[Double], v(1).asInstanceOf[Double])).foreach(println)

///// foldLeft

fc.describe("latitude").show
fc.select($"latitude", $"longitude").schema.fieldNames.map(fn => fc.describe(fn).show)

foldLeft(0)((x,y) => x+y) 

val p = Seq((0,1)).toDF().describe("_1").select("summary")
fc.select($"latitude", $"longitude").schema.fieldNames.map(fn => p.withColumn(fn, fc.describe(fn).apply(fn)))


p.withColumn("latitude", fc.describe("latitude").apply("latitude"))

val p = fc.describe("latitude")
val t = fc.describe("longitude")

p.join(t, p("summary") === t("summary"))

val mp = p.map(p => (p.summary, p))


val mp = p.rdd.keyBy(k => k(0).asInstanceOf[String]).mapValues(v => v(1).asInstanceOf[String])
val mt = t.rdd.keyBy(k => k(0).asInstanceOf[String]).mapValues(v => v(1).asInstanceOf[String])


rdd1 = sc.parallelize([("Id", 1),("Id", 2),("Id",3)])
rdd2 = sc.parallelize([("Result", 1),("Result", 0),("Result", 0)])

case class Elem(id: Int, result: Int)
val df = sqlCtx.createDataFrame(rdd1.zip(rdd2).map(x => Elem(x._1, x._2)))


/////
val p = fc.describe("latitude")
val t = fc.describe("longitude")

val mp = p.rdd.keyBy(k => k(0).asInstanceOf[String]).mapValues(v => v(1).asInstanceOf[String])
val mt = t.rdd.keyBy(k => k(0).asInstanceOf[String]).mapValues(v => v(1).asInstanceOf[String])

case class Elem(summary: String, vals: String)
val df = sqlContext.createDataFrame(mp.zip(mt).map(x => Elem(x._1, x._2)))

mp.zip(mt).map{ case((x:String,y:String),(u:String,v:String)) => (x,y,v)}

p.rdd.zip(t.select($"longitude").rdd).map{ case(Row(x:String, y:String), Row(v:String)) => (x,y,v) }










