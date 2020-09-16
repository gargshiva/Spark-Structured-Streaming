package problems

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object FlattenJsonStructure {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val inpDF = spark.read.json("/Users/shigarg1/Documents/ShivaHome/Github/Spark-Structured-Streaming/src/main/resources/sampleemp.json")
    inpDF.show(false)
    val op = flattenJsonStructure(inpDF)
    op.show(false)
  }

  def flattenJsonStructure(input: DataFrame): DataFrame = {
    val fields: Array[StructField] = input.schema.fields
    val fieldNames: Array[String] = fields.map(_.name)

    fields.foreach(f => {
      f.dataType match {
        case _: ArrayType =>
          val explodedDF = input
            .withColumn(colName = "explodedField", explode(col(f.name)))
            .drop(col(f.name))
            .withColumnRenamed("explodedField", f.name)
          return flattenJsonStructure(explodedDF)

        case s: StructType =>
          val childFieldNames = s.fieldNames.map(name => f.name + "." + name)
          val newFieldNames = fieldNames.filter(_ != f.name) ++ childFieldNames
          val renamedCols = newFieldNames.map(x => col(x))
          val explodedDF = input.select(renamedCols: _*)
          return flattenJsonStructure(explodedDF)

        case _ =>
      }
    })
    input
  }

}


