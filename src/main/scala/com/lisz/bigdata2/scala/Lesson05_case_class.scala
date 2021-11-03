package com.lisz.bigdata2.scala

class Dog (name:String, age:Int) {

}

case class Cat(name:String, age:Int) {

}

object Lesson05_case_class {
  def main(args: Array[String]): Unit = {
    val dog1 = new Dog("husky", 18)
    val dog2 = new Dog("husky", 18)
    println(dog1.equals(dog2))
    println(dog1 == dog2)

    val cat1 = Cat("tom", 20)
    val cat2 = Cat("tom", 20)
    println(cat1.equals(cat2))
    println(cat1 == cat2)
   }
}
