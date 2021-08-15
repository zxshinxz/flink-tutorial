package io.gizer

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object MovieLensRateCounterJob {
  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val result = env.readTextFile("path to file")
      .flatMap((event: String, out: Collector[(String, Int)]) => {
        try {
          val values = event.split(",")
          out.collect(values(2), 1)
        } catch {
          case e: Exception =>
        }
      })
      .groupBy(0)
      .sum(1)

    result.print()

    // execute program
    env.execute("MovieLens rate counter")
  }
}
