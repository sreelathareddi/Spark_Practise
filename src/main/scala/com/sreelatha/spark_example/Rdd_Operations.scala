package com.sreelatha.spark_example
package com.sreelatha.spark_example

import org.apache.spark.sql.SparkSession

object Rdd_Operations extends App{
  val session = SparkSession.builder().master("local[*]").appName("HelloWorld").getOrCreate();
  val sc=session.sparkContext
  val rdd = sc.parallelize(Seq(1,2,3,10,11,15,2));
  val rdd2=rdd.map(x=>x+1)
  val fileRdd=session.sparkContext.textFile("file:///C:/install/ExampleFiles/abc.txt")
  /*val display=fileRdd.collect()
  display.foreach(println)*/
  val splitRdd=fileRdd.flatMap(word=>word.split(" "))
  println(splitRdd.count())

  val wordRdd=splitRdd.map(word=>(word,1))
  val numWords=wordRdd.reduceByKey(_+_)
  numWords.foreach(print)

 //Console.in.readLine()
  session.stop()
}
