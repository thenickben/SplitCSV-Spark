//++++++++++++++++++++++++++++++++++++
// Split big file into N smaller ones
// Author: Nicolas Borrajo
//
//Note: worked example with N=2
//++++++++++++++++++++++++++++++++++++

// Start a Spark Session
import org.apache.spark.sql.SparkSession
import scala.util.Random

object SplitCSV {
	def main(args: Array[String]): Unit = {
	    val spark = SparkSession
	      .builder
	      .appName("SplitCSV_Session")
	      .getOrCreate

	    spark.sparkContext.setLogLevel("INFO")

		// Create a DataFrame from Spark Session read csv
		// (to simplify, works on local path/folder)
		var test_df = spark.read
					  .option("header","true")
					  .option("inferSchema","true")
					  .csv("test.csv")

		val column_names=df.columns // get column names
		val N=2 //Number of splits
		val weights=Array.fill(N)(1.0) //Weights (randomSplit will normalize)

		val splitDF = df.randomSplit(weights) //Split by weights
		val (df1,df2) = (splitDF(0),splitDF(1))

		//write splitted files into disk
		df1.write.format("csv").save("df1")
		df2.write.format("csv").save("df2")

	spark.stop() //Close Session
	}
}
