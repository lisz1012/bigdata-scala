package com.lisz.bigdata.scala

object Lesson05_Case_Class {
  // 中间件消息用的比较多： Spark中Master和Worker；Driver和Executor之间
  case  class Dog(name:String, age:Int){

  }

  def main(args: Array[String]): Unit = {
    val dog1 = new Dog("Husky", 18)
    val dog2 = Dog("Husky", 18) //new 可以省略
    println(dog1.equals(dog2))
    println(dog1 ==  dog2)
  }

}
