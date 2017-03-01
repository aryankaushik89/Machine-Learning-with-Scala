import org.apache.log4j.Logger
import org.apache.log4j.Level
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

sqlContext.sql("use titanic")
val t = sqlContext.table("titanic_train")

t.printSchema
/*
root
 |-- passengerid: string (nullable = true)
 |-- survived: double (nullable = true)
 |-- pclass: integer (nullable = true)
 |-- gender: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- sibsp: integer (nullable = true)
 |-- parch: integer (nullable = true)
 |-- ticket: string (nullable = true)
 |-- fare: double (nullable = true)
 |-- cabin: string (nullable = true)
 |-- embarked: string (nullable = true)
*/

///// data exploration, profiling
t.describe("age").show

///// feature transformation
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation._

// transform gender to numeric index
val g_ind = new StringIndexer().
  setInputCol("gender").
  setOutputCol("gender_ind").
  fit(t)
val t_g_ind = g_ind.transform(t)

// transform port of embarkation to numeric index
val e_ind = new StringIndexer().
  setInputCol("embarked").
  setOutputCol("embarked_ind").
  fit(t_g_ind)
val t_ind = e_ind.transform(t_g_ind)

// generate features list
val features = t_ind.schema.fieldNames.filter(x => x == "pclass" || x == "age" || x == "sibsp" || x == "parch" || x == "fare" || x == "gender_ind" || x == "embarked_ind")
// alternatively define your own array for included features
val features = Array("pclass", "age", "sibsp", "parch", "fare", "gender_ind", "embarked_ind")

// fill in null values as 0
val t_imp = t_ind.na.fill(0)

// put all the features into one vector called "features"
val t_vec = new VectorAssembler().
              setInputCols(features).
              setOutputCol("features").
              transform(t_imp)



/////// Modeling

// train the model using training data
val model = (new LogisticRegression()).setLabelCol("survived").fit(t_vec)

// score the model 
val predictions = model.transform(t_vec)

// show some predicted results
predictions.select($"passengerid", $"survived", $"features", $"probability", $"prediction").show

// calculate accuracy
val accuracy = predictions.filter($"survived" === $"prediction").count.toDouble/t.count

// evaluation metrics
val evaluator = new BinaryClassificationEvaluator().
  setLabelCol("survived").
  setMetricName("areaUnderROC")
val auc = evaluator.evaluate(predictions)

// check parameters and possible values
evaluator.explainParams



