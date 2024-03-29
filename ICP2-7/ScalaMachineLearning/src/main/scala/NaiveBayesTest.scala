import org.apache.spark.ml._
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object NaiveBayesTest {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local[4]")
      .appName("example")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    //load train df
    val df1 = sparkSession.read.option("header", "true").option("inferSchema", "true").csv(".\\data\\train.csv")
    df1.printSchema()

    val df = df1.na.drop()
    //handle missing values
    val meanValue = df.agg(mean(df("Age"))).first.getDouble(0)
    val fixedDf = df.na.fill(meanValue, Array("Age"))
    //test and train split
    val dfs = fixedDf.randomSplit(Array(0.7, 0.3))
    val trainDf = dfs(0).withColumnRenamed("Survived", "label")
    val crossDf = dfs(1)
//
//    // create pipeline stages for handling categorical
    val genderStages = handleCategorical("Sex")
    val embarkedStages = handleCategorical("Embarked")
    val pClassStages = handleCategorical("Pclass")
//
//    //columns for training
    val cols = Array("Sex_onehot", "Embarked_onehot", "Pclass_onehot", "SibSp", "Parch", "Age", "Fare")
    val vectorAssembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
//
//    //algorithm stage
    val naiveBayes = new NaiveBayes()

//    //pipeline
    val preProcessStages = genderStages ++ embarkedStages ++ pClassStages ++ Array(vectorAssembler)



    val pipeline = new Pipeline().setStages(preProcessStages ++ Array(naiveBayes))


//
    val model = pipeline.fit(trainDf)

    println("train accuracy with pipeline" + MLUtilities.accuracyScore(model.transform(trainDf), "label", "prediction"))
    println("test accuracy with pipeline" + MLUtilities.accuracyScore(model.transform(crossDf), "Survived", "prediction"))

  }

  def generateOutputFile(testDF: DataFrame, model: Model[_]) = {
    val scoredDf = model.transform(testDF)
    val outputDf = scoredDf.select("PassengerId", "prediction")
    val castedDf = outputDf.select(outputDf("PassengerId"), outputDf("prediction").cast(IntegerType).as("Survived"))
    castedDf.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save("data/output/")
  }

  def crossValidation(pipeline: Pipeline, paramMap: Array[ParamMap], df: DataFrame): Model[_] = {
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramMap)
      .setNumFolds(5)
    cv.fit(df)
  }

  def handleCategorical(column: String): Array[PipelineStage] = {
    val stringIndexer = new StringIndexer().setInputCol(column)
      .setOutputCol(s"${column}_index")
      .setHandleInvalid("skip")
    val oneHot = new OneHotEncoder().setInputCol(s"${column}_index").setOutputCol(s"${column}_onehot")
    Array(stringIndexer, oneHot)
  }

}