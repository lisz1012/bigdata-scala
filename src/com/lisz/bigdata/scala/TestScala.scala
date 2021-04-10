package com.lisz.bigdata.scala

// In Scala, the main method must be put in an object, which is a static singleton class in Java
// "Before" and "After" will be executed first, they are like the static or constructor block in java
// Compiler, by default, created a static/constructor block and put the to lines into it.
object TestScala {
  private var b:B = new B("female")
  val name = "Alice"

  println("Before")
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    b.printMsg()
  }
  println("After")
}

object A{
  private var b:B = new B(100)
  def main(args: Array[String]): Unit = {
    println("Hello from a.")
    println(s"Class Name is: ${b.name}")
    println(s"Object Name is: ${B.name}")
    b.print()
  }
}

// The below "gender:String" by default is a property for the class, and it's private and val
class B(gender:String){
  var name = 12
  def this(name:Int) {
    this("female")
    this.name = name
  }
  var a:Int = 3
  println(s"Before ...$a...")
  def printMsg(): Unit = {
    println(s"msg from ${TestScala.name} to $name")
    println(s"$name gender is $gender")
  }
  println(s"After...${a+3}...")
  def print(): Unit = {
    println(s"${B.name}")
  }
}

object B{
  val name = 13
}
