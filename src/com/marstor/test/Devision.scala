package com.marstor.test

/**
  * Created by root on 11/2/16.
  */
case class Division(val number: Int) {
  def unapply(divider: Int): Option[(Int, Int)] =
    if (number % divider == 0) Some(number / divider, 0) else None

  def unapply(divider: Double): Boolean = number % divider.toInt == 0

  def unapply(x: Float): Option[Seq[Int]] = {
    val seq = (3 to 10).map(i => i * x.toInt)
    println(seq)
    Some(seq)
  }

//  def main(args: Array[String]) {
//    val u = 5.0F match {
//      case divisionOf15(Seq(f1, f2, _*)) => println(s"$f1, $f2")
//    }
//  }
}



