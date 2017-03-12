package example

/**
  * Created by DoogieMin on 2017. 3. 13..
  */
object Main {
  def main(args: Array[String]): Unit = {
    val a = List(1, 2, 3, 4)
    println(a.groupBy(int => int % 2 == 0))

  }
}
