```scala
val wordsList = List("cat", "elephant", "rat", "rat", "cat")
val wordsRDD = sc.parallelize(wordsList)

// Print out the type of wordsRDD
println(wordsRDD.getClass)
```


><pre>
> class org.apache.spark.rdd.ParallelCollectionRDD
> wordsList: List[String] = List(cat, elephant, rat, rat, cat)
> wordsRDD: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[14] at parallelize at <console>:82
> <pre>




```scala
def makePlural(word: String): String = {
//     """Adds an 's' to `word`.
//     Note:
//         This is a simple function that only adds an 's'.  No attempt is made to follow proper
//         pluralization rules.
//     Args:
//         word (str): A string.
//     Returns:
//         str: A string with 's' added to it.
//     """
    
    return word+"s"
}

println(makePlural("cat"))
```


><pre>
> cats
> makePlural: (word: String)String
> <pre>




```scala
val pluralRDD = wordsRDD.map(s=>makePlural(s))
pluralRDD.collect().foreach(println)
```


><pre>
> cats
> elephants
> rats
> rats
> cats
> pluralRDD: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[15] at map at <console>:92
> <pre>




```scala
val pluralLambdaRDD = wordsRDD.map((s:String)=>s+'s')
pluralLambdaRDD.collect().foreach(println)
```


><pre>
> cats
> elephants
> rats
> rats
> cats
> pluralLambdaRDD: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[16] at map at <console>:88
> <pre>




```scala
val pluralLengths = pluralRDD.map((s:String)=> (s,s.length)).collect()
pluralLengths.foreach(println)
```


><pre>
> (cats,4)
> (elephants,9)
> (rats,4)
> (rats,4)
> (cats,4)
> pluralLengths: Array[(String, Int)] = Array((cats,4), (elephants,9), (rats,4), (rats,4), (cats,4))
> <pre>




```scala
val wordPairs = wordsRDD.map((s:String)=> (s,1))
wordPairs.collect().foreach(println)
```


><pre>
> (cat,1)
> (elephant,1)
> (rat,1)
> (rat,1)
> (cat,1)
> wordPairs: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[18] at map at <console>:88
> <pre>




```scala
// Note that groupByKey requires no parameters
val wordsGrouped = wordPairs.groupByKey()
wordsGrouped.collect().foreach(println)
```


><pre>
> (elephant,CompactBuffer(1))
> (rat,CompactBuffer(1, 1))
> (cat,CompactBuffer(1, 1))
> wordsGrouped: org.apache.spark.rdd.RDD[(String, Iterable[Int])] = ShuffledRDD[19] at groupByKey at <console>:93
> <pre>




```scala
val wordCountsGrouped = wordsGrouped.map(x => (x._1,(x._2).foldLeft(0)((a,b)=>a+b)))
wordCountsGrouped.collect().foreach(println)
```


><pre>
> (elephant,1)
> (rat,2)
> (cat,2)
> wordCountsGrouped: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[20] at map at <console>:96
> <pre>




```scala
val wordCounts = wordPairs.reduceByKey((count1,count2)=>count1+count2)
wordCounts.collect().foreach(println)
```


><pre>
> (elephant,1)
> (rat,2)
> (cat,2)
> wordCounts: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[21] at reduceByKey at <console>:92
> <pre>




```scala
val wordCountsCollected = wordsRDD.map((s:String)=> (s,1)).reduceByKey((count1,count2)=>count1+count2).collect()
wordCountsCollected.foreach(println)
```


><pre>
> (elephant,1)
> (rat,2)
> (cat,2)
> wordCountsCollected: Array[(String, Int)] = Array((elephant,1), (rat,2), (cat,2))
> <pre>




```scala
val uniqueWords = wordsRDD.distinct.count
println(uniqueWords)
```


><pre>
> 3
> uniqueWords: Long = 3
> <pre>




```scala
val totalCount = wordCounts.map(tuple=>tuple._2.toInt).reduce((v1,v2)=>v1+v2)
val average = totalCount / uniqueWords.toFloat

println(totalCount)
println(average)
```


><pre>
> 5
> 1.6666666
> totalCount: Int = 5
> average: Float = 1.6666666
> <pre>




```scala
// TODO: Replace <FILL IN> with appropriate code
import org.apache.spark.rdd.RDD

def wordCount(wordListRDD: RDD[String]): RDD[(String, Int)] = {
    wordListRDD.map((s:String)=> (s,1)).reduceByKey((count1,count2)=>count1+count2)
}

wordCount(wordsRDD).collect().foreach(println)
```


><pre>
> (elephant,1)
> (rat,2)
> (cat,2)
> import org.apache.spark.rdd.RDD
> wordCount: (wordListRDD: org.apache.spark.rdd.RDD[String])org.apache.spark.rdd.RDD[(String, Int)]
> <pre>




```scala
// Just run this code
import scala.util.matching

def removePunctuation(text: String): String = {
    text.replaceAll("""\p{Punct}|^\s+|\s+$""", "").toLowerCase
}  

println(removePunctuation("Hi, you!"))
println(removePunctuation(" No under_score!"))
```


><pre>
> hi you
> no underscore
> import scala.util.matching
> removePunctuation: (text: String)String
> <pre>




```scala
// Just run this code
val fileName="/home/akash/kth/data_intensive/labs/id2221/lab2/data/story/shakespeare.txt"
val shakespeareRDD = sc.textFile(fileName, 8).map(removePunctuation)
shakespeareRDD.zipWithIndex().take(15).map(x => (x._2 + 1) + ": " + x._1).foreach(println)
```


><pre>
> 1: 1609
> 2: 
> 3: the sonnets
> 4: 
> 5: by william shakespeare
> 6: 
> 7: 
> 8: 
> 9: 1
> 10: from fairest creatures we desire increase
> 11: that thereby beautys rose might never die
> 12: but as the riper should by time decease
> 13: his tender heir might bear his memory
> 14: but thou contracted to thine own bright eyes
> 15: feedst thy lights flame with selfsubstantial fuel
> fileName: String = /home/akash/kth/data_intensive/labs/id2221/lab2/data/story/shakespeare.txt
> shakespeareRDD: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[46] at map at <console>:89
> <pre>




```scala
//test zip with Index
print(List("spark","flink","storm").zipWithIndex)
shakespeareRDD.take(5).foreach(println)
```


><pre>
> List((spark,0), (flink,1), (storm,2))
> 
> 
> 
> doctype html
> <pre>




```scala
val shakespeareWordsRDD = shakespeareRDD.flatMap(x=>x.split("\\s"))
val shakespeareWordCount = shakespeareWordsRDD.count()

shakespeareWordsRDD.top(5).foreach(println)
println(shakespeareWordCount)
```


><pre>
> zwaggerd
> zounds
> zounds
> zounds
> zounds
> 927633
> shakespeareWordsRDD: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[54] at flatMap at <console>:91
> shakespeareWordCount: Long = 927633
> <pre>




```scala
val shakeWordsRDD = shakespeareWordsRDD.filter(word => word != "")
val shakeWordCount = shakeWordsRDD.count()

println(shakeWordCount)
```


><pre>
> 882996
> shakeWordsRDD: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[57] at filter at <console>:93
> shakeWordCount: Long = 882996
> <pre>




```scala
//val top15WordsAndCounts = wordCount(shakeWordsRDD).sortBy(x=>x._2,false).take(15).map(x => (x._2, x._1))
val top15WordsAndCounts = wordCount(shakeWordsRDD).top(15)(Ordering[Int].on(x=>x._2)).map(x => (x._2, x._1))
top15WordsAndCounts.map(x => x._1 + ": " + x._2).foreach(println)
```


><pre>
> 27361: the
> 26028: and
> 20681: i
> 19150: to
> 17463: of
> 14593: a
> 13615: you
> 12481: my
> 10956: in
> 10890: that
> 9134: is
> 8497: not
> 7771: with
> 7769: me
> 7678: it
> top15WordsAndCounts: Array[(Int, String)] = Array((27361,the), (26028,and), (20681,i), (19150,to), (17463,of), (14593,a), (13615,you), (12481,my), (10956,in), (10890,that), (9134,is), (8497,not), (7771,with), (7769,me), (7678,it))
> <pre>