package org.apache.flink.api.scala.extensions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{JoinDataSet, CoGroupDataSet, GroupedDataSet, DataSet}

import scala.reflect.ClassTag

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
  *       ds.filterWith {
  *         case Point(x, _) => x > 1
  *       }.reduceWith {
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
  *
  */
package object acceptPartialFunctions {

  implicit class OnDataSet[T: TypeInformation](dataset: DataSet[T]) {

    /**
      * Applies a function `fun` to each item of the Dataset
      *
      * @param fun The function to be applied to each item
      * @tparam R The type of the items in the returned collection
      * @return A dataset of R
      */
    def mapWith[R: TypeInformation: ClassTag](fun: T => R): DataSet[R] =
      dataset.map(fun)

    /**
      * Applies a function `fun` to a partition as a whole
      *
      * @param fun The function to be applied on the whole partition
      * @tparam R The type of the items in the returned collection
      * @tparam P A generic iterable representing the data in the partition
      * @return A dataset of R
      */
    def mapPartitionWith[R: TypeInformation: ClassTag, P[_: TypeInformation: ClassTag] <: Iterable](
                                                                                                     fun: P[T] => R): DataSet[R] =
      dataset.mapPartition {
        (it, out) =>
          out.collect(fun(it.to[P]))
      }

    /**
      * Applies a function `fun` to each item of the dataset, producing a collection of items
      * that will be flattened in the resulting Dataset
      *
      * @param fun The function to be applied to each item
      * @tparam R The type of the items in the returned collection
      * @return A dataset of R
      */
    def flatMapWith[R: TypeInformation: ClassTag](fun: T => TraversableOnce[R]): DataSet[R] =
      dataset.flatMap(fun)

    /**
      * Applies a predicate `fun` to each item of the dataset, keeping only those for which
      * the predicate holds
      *
      * @param fun The predicate to be tested on each item
      * @return A dataset of R
      */
    def filterWith(fun: T => Boolean): DataSet[T] =
      dataset.filter(fun)

    /**
      * Applies a reducer `fun` to each item of the dataset
      *
      * @param fun The reducing function to be applied on the whole dataset
      * @tparam R The type of the items in the returned collection
      * @return A dataset of R
      */
    def reduceWith[R: TypeInformation: ClassTag](fun: (T, T) => T): DataSet[T] =
      dataset.reduce(fun)

    /**
      * Applies a reducer `fun` to a grouped dataset
      *
      * @param fun The function to be applied to the whole grouping
      * @tparam R The type of the items in the returned collection
      * @tparam G A generic iterable representing the data in the grouping
      * @return A dataset of R
      */
    def reduceGroupWith[R: TypeInformation: ClassTag, G[_: TypeInformation: ClassTag] <: Iterable](
        fun: G[T] => R): DataSet[R] =
      dataset.reduceGroup {
        (it, out) =>
          out.collect(fun(it.to[G]))
      }

    /**
      * Groups the items according to a grouping function `fun`
      *
      * @param fun The grouping function
      * @tparam K The return type of the grouping function, for which type information must be known
      * @return A grouped dataset of T
      */
    def groupingBy[K: TypeInformation: ClassTag](fun: T => K): GroupedDataSet[T] =
      dataset.groupBy(fun)

  }

  implicit class OnJoinDataSet[L: TypeInformation, R: TypeInformation](
      dataset: JoinDataSet[L, R]) {

  }

  implicit class OnCoGroupDataSet[L: TypeInformation, R: TypeInformation](
      dataset: CoGroupDataSet[L, R]) {

  }

}
