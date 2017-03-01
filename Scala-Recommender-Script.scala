import org.apache.log4j.Logger
import org.apache.log4j.Level
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

import org.apache.spark.ml.recommendation.ALS

sqlContext.sql("use movielens")
val trainingData = sqlContext.table("training")
val testData = sqlContext.table("test")

val model = (new ALS().
               setUserCol("userid").
               setItemCol("itemid").
               setRatingCol("rating").
               setPredictionCol("pred")).
            fit(trainingData)
val scores = model.transform(testData)



