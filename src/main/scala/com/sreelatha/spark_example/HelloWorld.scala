package com.sreelatha.spark_example

import org.apache.spark.sql.SparkSession;

object HelloWorld {
def main(args:Array[String]): Unit ={
  println("welcome to spark")
  val session = SparkSession.builder().master("local[*]").appName("HelloWorld").getOrCreate();

  //Console.in.readLine()
  session.stop()

}
  def strDuplicateRemoval:String={
    val str="aabcddecdab"
    var str1=""
    for(value<-str){
      if(str1.indexOf(value)== -1){
        str1+=value
      }
    }
    return(str1)
  }
}
