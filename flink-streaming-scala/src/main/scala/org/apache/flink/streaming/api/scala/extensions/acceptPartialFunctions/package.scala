package org.apache.flink.streaming.api.scala.extensions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{JoinedStreams, CoGroupedStreams, KeyedStream, DataStream}
import org.apache.flink.streaming.api.windowing.windows.Window

import scala.reflect.ClassTag

/**
  * acceptPartialFunctions extends the original DataStream with methods with unique names
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
  *       val env = StreamExecutionEnvironment.getExecutionEnvironment
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

  implicit class OnDataStream[T: TypeInformation](stream: DataStream[T]) {

    /**
      * Applies a function `fun` to each item of the stream
      *
      * @param fun The function to be applied to each item
      * @tparam R The type of the items in the returned stream
      * @return A dataset of R
      */
    def mapWith[R: TypeInformation: ClassTag](fun: T => R): DataStream[R] =
      stream.map(fun)

    /**
      * Applies a function `fun` to each item of the stream, producing a collection of items
      * that will be flattened in the resulting stream
      *
      * @param fun The function to be applied to each item
      * @tparam R The type of the items in the returned stream
      * @return A dataset of R
      */
    def flatMapWith[R: TypeInformation: ClassTag](fun: T => TraversableOnce[R]): DataStream[R] =
      stream.flatMap(fun)

    /**
      * Applies a predicate `fun` to each item of the stream, keeping only those for which
      * the predicate holds
      *
      * @param fun The predicate to be tested on each item
      * @return A dataset of R
      */
    def filterWith(fun: T => Boolean): DataStream[T] =
      stream.filter(fun)

    /**
      * Keys the items according to a keying function `fun`
      *
      * @param fun The keying function
      * @tparam K The type of the key, for which type information must be known
      * @return A stream of Ts keyed by Ks
      */
    def keyingBy[K: TypeInformation: ClassTag](fun: T => K): KeyedStream[T, K] =
      stream.keyBy(fun)

  }

  implicit class OnKeyedStream[T: TypeInformation, K](stream: KeyedStream[T, K]) {

    /**
      * Applies a reducer `fun` to the stream
      *
      * @param fun The reducing function to be applied on the keyed stream
      * @return A data set of Ts
      */
    def reduceWith(fun: (T, T) => T): DataStream[T] =
      stream.reduce(fun)

  }

  implicit class OnJoinedStream[L: TypeInformation, R: TypeInformation, K, W <: Window](
    stream: JoinedStreams.WithWindow[L, R, K, W]) {

    /**
      * Completes the join operation with the user function that is executed
      * for windowed groups.
      *
      * @param fun The function that defines the projection of the join
      * @tparam O The return type of the projection, for which type information must be known
      * @return A fully joined data set of Os
      */
    def projecting[O: TypeInformation: ClassTag](fun: (L, R) => O): DataStream[O] =
      stream.apply(fun)

  }

}
