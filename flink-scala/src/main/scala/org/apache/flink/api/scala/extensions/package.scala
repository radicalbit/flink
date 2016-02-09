package org.apache.flink.api.scala

import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.reflect.ClassTag

package object extensions {

  /**
    * AcceptPartialFunctions extends the original DataSet with methods with unique names
    * that delegate to core higher-order functions (e.g. `map`) so that we can work around
    * the fact that overloaded methods taking functions as parameters can't accept partial
    * functions as well. This enables the possibility to directly apply pattern matching
    * to inputs to destructure inputs (e.g. tuple extraction).
    *
    * e.g.
    * {{{
    *   object Main {
    *     import org.apache.flink.api.scala.extensions._
    *     case class Point(x: Double, y: Double)
    *     def main(args: Array[String]): Unit = {
    *       val env = ExecutionEnvironment.getExecutionEnvironment
    *       val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
    *       ds.reduceWith {
    *         case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
    *       }.mapWith {
    *         case Point(x, y) => (x, y)
    *       }.flatMapWith {
    *         case (x, y) => Seq('x' -> x, 'y' -> y)
    *       }.groupingBy {
    *         case (id, value) => id
    *       }
    *     }
    *   }
    * }}}
    * @param dataset The dataset extended by this implicit class
    * @tparam T The type of the input dataset (for which TypeInformation must be known)
    */
  implicit class AcceptPartialFunctions[T: TypeInformation](dataset: DataSet[T]) {

    def mapWith[R: TypeInformation: ClassTag](fun: T => R): DataSet[R] =
      dataset.map(fun)

    def flatMapWith[R: TypeInformation: ClassTag](fun: T => TraversableOnce[R]): DataSet[R] =
      dataset.flatMap(fun)

    def reduceWith[R: TypeInformation: ClassTag](fun: (T, T) => T): DataSet[T] =
      dataset.reduce(fun)

    def groupingBy[K: TypeInformation: ClassTag](fun: T => K): GroupedDataSet[T] =
      dataset.groupBy(fun)

  }

}
