package com.randomgraphs.saferdd

import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

case class DirtyValue(value: Any, exception: Exception)

class SafeFunction[S, T](val f: S => T) extends Function[S, Either[T, DirtyValue]] {
  def apply(input: S): Either[T, DirtyValue] = {
    try {
      Left(f(input))
    } catch {
      case e: Exception => Right(DirtyValue(input, e))
    }
  }
}

class SafeRDD[T: ClassTag](val rdd: RDD[Either[T, DirtyValue]]) extends RDD[Either[T, DirtyValue]](rdd) {

  def clean(): RDD[T] = {
    rdd.flatMap(x => x match {
      case Left(t) => List(t)
      case Right(dv) => Nil
    })
  }

  def dirty(): RDD[DirtyValue] = {
    rdd.flatMap(x => x match {
      case Left(t) => Nil
      case Right(dv) => List(dv)
    })
  }

  def map[U: ClassTag](f: T => U): SafeRDD[U] = {
    val sf = new SafeFunction(f)
    new SafeRDD[U](rdd.map(x => x.fold(sf, dv => Right(dv))))
  }

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): SafeRDD[U] = {
    val sf = new SafeFunction(f)
    new SafeRDD[U](rdd.flatMap(x => x match {
      case Left(t) => {
        sf(t) match {
          case Left(tu) => tu.map(u => Left(u))
          case Right(dv) => List(Right(dv))
        }
      }
      case Right(dv) => List(Right(dv))
    }))
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Either[T, DirtyValue]] = {
    rdd.compute(split, context)
  }

  override protected def getPartitions: Array[Partition] = rdd.partitions
}

object SafeRDD {
  implicit def safeRDD[T: ClassTag](rdd: RDD[T]): SafeRDD[T] = {
    new SafeRDD[T](rdd.map(t => Left(t)))
  }
}
