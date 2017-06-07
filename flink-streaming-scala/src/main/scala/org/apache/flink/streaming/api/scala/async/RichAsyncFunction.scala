package org.apache.flink.streaming.api.scala.async

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.functions.AbstractRichFunction

/**
  *
  * @tparam IN The type of the input element
  * @tparam OUT The type of the output elements
  */
@PublicEvolving
abstract class RichAsyncFunction[IN, OUT] extends AbstractRichFunction with AsyncFunction[IN, OUT] {
  /**
    * Trigger the async operation for each stream input
    *
    * @param input     element coming from an upstream task
    * @param collector to collect the result data
    */
  override def asyncInvoke(input: IN, collector: AsyncCollector[OUT]): Unit = ???
}
