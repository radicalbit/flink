package org.apache.flink.api.scala.extensions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * acceptPartialFunctions extends the original DataSet with methods with unique names
  * that delegate to core higher-order functions (e.g. `map`) so that we can work around
  * the fact that overloaded methods taking functions as parameters can't accept partial
  * functions as well. This enables the possibility to directly apply pattern matching
  * to decompose inputs such as tuples, case classes and collections.
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

  implicit class OnDataSet[T: TypeInformation](ds: DataSet[T]) {

    /**
      * Applies a function `fun` to each item of the data set
      *
      * @param fun The function to be applied to each item
      * @tparam R The type of the items in the returned data set
      * @return A dataset of R
      */
    def mapWith[R: TypeInformation: ClassTag](fun: T => R): DataSet[R] =
      ds.map(fun)

    /**
      * Applies a function `fun` to a partition as a whole
      *
      * @param fun The function to be applied on the whole partition
      * @tparam R The type of the items in the returned data set
      * @return A dataset of R
      */
    def mapPartitionWith[R: TypeInformation: ClassTag](fun: Seq[T] => R): DataSet[R] =
      ds.mapPartition {
        (it, out) =>
          out.collect(fun(it.to[Seq]))
      }

    /**
      * Applies a function `fun` to each item of the dataset, producing a collection of items
      * that will be flattened in the resulting data set
      *
      * @param fun The function to be applied to each item
      * @tparam R The type of the items in the returned data set
      * @return A dataset of R
      */
    def flatMapWith[R: TypeInformation: ClassTag](fun: T => TraversableOnce[R]): DataSet[R] =
      ds.flatMap(fun)

    /**
      * Applies a predicate `fun` to each item of the data set, keeping only those for which
      * the predicate holds
      *
      * @param fun The predicate to be tested on each item
      * @return A dataset of R
      */
    def filterWith(fun: T => Boolean): DataSet[T] =
      ds.filter(fun)

    /**
      * Applies a reducer `fun` to the data set
      *
      * @param fun The reducing function to be applied on the whole data set
      * @tparam R The type of the items in the returned collection
      * @return A data set of Rs
      */
    def reduceWith[R: TypeInformation: ClassTag](fun: (T, T) => T): DataSet[T] =
      ds.reduce(fun)

    /**
      * Applies a reducer `fun` to a grouped data set
      *
      * @param fun The function to be applied to the whole grouping
      * @tparam R The type of the items in the returned data set
      * @return A dataset of Rs
      */
    def reduceGroupWith[R: TypeInformation: ClassTag](fun: Seq[T] => R): DataSet[R] =
      ds.reduceGroup {
        (it, out) =>
          out.collect(fun(it.to[Seq]))
      }

    /**
      * Groups the items according to a grouping function `fun`
      *
      * @param fun The grouping function
      * @tparam K The return type of the grouping function, for which type information must be known
      * @return A grouped data set of Ts
      */
    def groupingBy[K: TypeInformation: ClassTag](fun: T => K): GroupedDataSet[T] =
      ds.groupBy(fun)

  }

  implicit class OnJoinDataSet[L: TypeInformation, R: TypeInformation](
      dataset: JoinDataSet[L, R]) {

    /**
      * Joins the data sets using the function `fun` to project elements from both in the
      * resulting data set
      *
      * @param fun The function that defines the projection of the join
      * @tparam O The return type of the projection, for which type information must be known
      * @return A fully joined data set of Os
      */
    def projecting[O: TypeInformation: ClassTag](fun: (L, R) => O): DataSet[O] =
      dataset(fun)

  }

  implicit class OnCoGroupDataSet[L: TypeInformation, R: TypeInformation](
      dataset: CoGroupDataSet[L, R]) {

    /**
      * Co-groups the data sets using the function `fun` to project elements from both in
      * the resulting data set
      *
      * @param fun The function that defines the projection of the co-group operation
      * @tparam O The return type of the projection, for which type information must be known
      * @return A fully co-grouped data set of Os
      */
    def projecting[O: TypeInformation: ClassTag](fun: (Seq[L], Seq[R]) => O): DataSet[O] =
      dataset {
        (left, right) =>
          fun(left.to[Seq], right.to[Seq])
      }

  }

}
