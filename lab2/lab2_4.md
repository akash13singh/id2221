```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)

val df = sqlContext.read.json("/home/akash/kth/data_intensive/labs/id2221/lab2/data/people/people.json")
df.show()
```


><pre>
> The cell was cancelled.
> <pre>



```scala
df.printSchema
```


><pre>
> root
>  |-- age: long (nullable = true)
>  |-- name: string (nullable = true)
> <pre>




```scala
df.select("name").show()
```


><pre>
> +-------+
> |   name|
> +-------+
> |Michael|
> |   Andy|
> | Justin|
> +-------+
> <pre>




```scala
// Select everybody, but increment the age by 1
df.select($"name",($"age")+1).show()
```


><pre>
> +-------+---------+
> |   name|(age + 1)|
> +-------+---------+
> |Michael|     null|
> |   Andy|       31|
> | Justin|       20|
> +-------+---------+
> <pre>




```scala
// Select people older than 21
df.filter($"age">21).show()

// Count people by age
df.groupBy("age").count().show()
```


><pre>
> +---+----+
> |age|name|
> +---+----+
> | 30|Andy|
> +---+----+
> 
> +----+-----+
> | age|count|
> +----+-----+
> |  19|    1|
> |null|    1|
> |  30|    1|
> +----+-----+
> <pre>




```scala
df.registerTempTable("df")
sqlContext.sql("select * from df").show()
```


><pre>
> <console>:82: warning: method registerTempTable in class Dataset is deprecated: Use createOrReplaceTempView(viewName) instead.
>               df.registerTempTable("df")
>                  ^
> +----+-------+
> | age|   name|
> +----+-------+
> |null|Michael|
> |  30|   Andy|
> |  19| Justin|
> +----+-------+
> <pre>




```scala
// TODO: Replace <FILL IN> with appropriate code
val sqlC = new org.apache.spark.sql.SQLContext(sc)
import sqlC.implicits._
case class Person(name: String, age: Int)

// Load a text file and convert each line to a Row.
val lines = sc.textFile("/home/akash/kth/data_intensive/labs/id2221/lab2/data/people/people.txt")
val parts = lines.map(l => l.split(","))
val people = parts.map(p => Person(p(0), p(1).trim.toInt))


// Infer the schema, and register the DataFrame as a table.
val schemaPeople = people.toDF()
schemaPeople.registerTempTable("people")

// SQL can be run over DataFrames that have been registered as a table. Complete the following query
// to return teenagers, i.e., age >= 13 and age <= 19.
val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >=13 and aGE<=19")

// The results of SQL queries are DataFrames and support all the normal RDD operations.
// The columns of a row in the result can be accessed by field index:
teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

// or by field name:
teenagers.map(t => "Name: " + t.getAs("name").toString).collect().foreach(println)
```


><pre>
> <console>:33: warning: constructor SQLContext in class SQLContext is deprecated: Use SparkSession.builder instead
>               val sqlC = new org.apache.spark.sql.SQLContext(sc)
>                          ^
> <console>:45: warning: method registerTempTable in class Dataset is deprecated: Use createOrReplaceTempView(viewName) instead.
>               schemaPeople.registerTempTable("people")
>                            ^
> Name: Justin
> Name: Justin
> sqlC: org.apache.spark.sql.SQLContext = org.apache.spark.sql.SQLContext@2237e4b
> import sqlC.implicits._
> defined class Person
> lines: org.apache.spark.rdd.RDD[String] = /home/akash/kth/data_intensive/labs/id2221/lab2/data/people/people.txt MapPartitionsRDD[26] at textFile at <console>:38
> parts: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[27] at map at <console>:39
> people: org.apache.spark.rdd.RDD[Person] = MapPartitionsRDD[28] at map at <console>:40
> schemaPeople: org.apache.spark.sql.DataFrame = [name: string, age: int]
> teenagers: org.apache.spark.sql.DataFrame = [name: string]
> <pre>




```scala
// Just run this code
// Create an RDD
val people = sc.textFile("/home/akash/kth/data_intensive/labs/id2221/lab2/data/people/people.txt")

// The schema is encoded in a string
val schemaString = "name age"

// Import Row.
import org.apache.spark.sql.Row;

// Import Spark SQL data types
import org.apache.spark.sql.types.{StructType,StructField,StringType};

// Generate the schema based on the string of schema
val schema =
  StructType(
    schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

// Convert records of the RDD (people) to Rows.
val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))

// Apply the schema to the RDD.
val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)

// Register the DataFrames as a table.
peopleDataFrame.registerTempTable("people")

// SQL statements can be run by using the sql methods provided by sqlContext.
val results = sqlContext.sql("SELECT name FROM people")

// The results of SQL queries are DataFrames and support all the normal RDD operations.
// The columns of a row in the result can be accessed by field index or by field name.
results.map(t => "Name: " + t(0)).collect().foreach(println)
```


><pre>
> <console>:118: warning: method registerTempTable in class Dataset is deprecated: Use createOrReplaceTempView(viewName) instead.
>               peopleDataFrame.registerTempTable("people")
>                               ^
> Name: Michael
> Name: Andy
> Name: Justin
> people: org.apache.spark.rdd.RDD[String] = /home/akash/kth/data_intensive/labs/id2221/lab2/data/people/people.txt MapPartitionsRDD[38] at textFile at <console>:95
> schemaString: String = name age
> import org.apache.spark.sql.Row
> import org.apache.spark.sql.types.{StructType, StructField, StringType}
> schema: org.apache.spark.sql.types.StructType = StructType(StructField(name,StringType,true), StructField(age,StringType,true))
> rowRDD: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[40] at map at <console>:112
> peopleDataFrame: org.apache.spark.sql.DataFrame = [name: string, age: string]
> results: org.apache.spark.sql.DataFrame = [name: string]
> <pre>




```scala
// Just run this code
// Load data from a parquet file
val df = sqlContext.read.load("/home/akash/kth/data_intensive/labs/id2221/lab2/data/people/people.parquet")
df.select("name", "favorite_color").write.mode("overwrite").save("namesAndFavColors.parquet")

// Manually specify the data source type, e.g., json, parquet, jdbc.
val jdf = sqlContext.read.format("json").load("/home/akash/kth/data_intensive/labs/id2221/lab2/data/people/people.json")
jdf.select("name", "age").write.format("parquet").mode("overwrite").save("namesAndAges.parquet")
```


><pre>
> df: org.apache.spark.sql.DataFrame = [name: string, favorite_color: string ... 1 more field]
> jdf: org.apache.spark.sql.DataFrame = [age: bigint, name: string]
> <pre>




```scala
// Just run this code
// The RDD is implicitly converted to a DataFrame by implicits, allowing it to be stored using Parquet.
schemaPeople.write.parquet("people.parquet")

// Read in the parquet file created above.  Parquet files are self-describing so the schema is preserved.
// The result of loading a Parquet file is also a DataFrame.
val parquetFile = sqlContext.read.parquet("people.parquet")

//Parquet files can also be registered as tables and then used in SQL statements.
parquetFile.registerTempTable("parquetFile")
val teenagers = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
```


><pre>
> <console>:108: warning: method registerTempTable in class Dataset is deprecated: Use createOrReplaceTempView(viewName) instead.
>               parquetFile.registerTempTable("parquetFile")
>                           ^
> Name: Justin
> parquetFile: org.apache.spark.sql.DataFrame = [name: string, age: int]
> teenagers: org.apache.spark.sql.DataFrame = [name: string]
> <pre>




```scala
// Just run this code
// A JSON dataset is pointed to by path.
// The path can be either a single text file or a directory storing text files.
val people = sqlContext.read.json("/home/akash/kth/data_intensive/labs/id2221/lab2/data/people/people.json")

// The inferred schema can be visualized using the printSchema() method.
people.printSchema()
// root
//  |-- age: integer (nullable = true)
//  |-- name: string (nullable = true)

// Register this DataFrame as a table.
people.registerTempTable("people")

// SQL statements can be run by using the sql methods provided by sqlContext.
val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

// Alternatively, a DataFrame can be created for a JSON dataset represented by
// an RDD[String] storing one JSON object per string.
val anotherPeopleRDD = sc.parallelize(
  """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
val anotherPeople = sqlContext.read.json(anotherPeopleRDD)
```


><pre>
> <console>:101: warning: method registerTempTable in class Dataset is deprecated: Use createOrReplaceTempView(viewName) instead.
> people.registerTempTable("people")
>        ^
> root
>  |-- age: long (nullable = true)
>  |-- name: string (nullable = true)
> 
> people: org.apache.spark.sql.DataFrame = [age: bigint, name: string]
> teenagers: org.apache.spark.sql.DataFrame = [name: string]
> anotherPeopleRDD: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[69] at parallelize at <console>:108
> anotherPeople: org.apache.spark.sql.DataFrame = [address: struct<city: string, state: string>, name: string]
> <pre>