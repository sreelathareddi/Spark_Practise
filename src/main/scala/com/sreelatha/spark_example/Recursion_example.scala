package com.sreelatha.spark_example
package com.sreelatha.spark_example

import scala.annotation.tailrec

object Recursion_example {
  def main(args:Array[String])={
    println(factorial(5))
    println(tail_Recursion(5,1))
    val str="sreelatha"
    val str1=identify_duplicates(str);
    println(str1)
  }
  def factorial(i:Int): Int ={
    var fact=1
    for(num<-1 to i){
      fact=fact*num
    }
    fact
  }
  import scala.annotation.tailrec
  @tailrec
  def tail_Recursion(i:Int,accumulator:Int):Int={
    if(i<=1) accumulator
    else tail_Recursion(i-1,accumulator*i)
  }

 /* def identify_duplicates(str:String):String={
    var str1=str.distinct
    return str1;
  }*/
}
