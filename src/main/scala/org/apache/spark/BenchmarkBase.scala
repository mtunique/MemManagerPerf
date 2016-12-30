package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Benchmark

/**
  * Created by taomeng on 2016/12/30.
  */
trait BenchmarkBase {

  lazy val sparkSession = SparkSession.builder
    .master("local[1]")
    .appName("microbenchmark")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.autoBroadcastJoinThreshold", 1)
    .getOrCreate()

  /** Runs function `f` with whole stage codegen on and off. */
  def runBenchmark(name: String, cardinality: Long)(f: => Unit): Unit = {
    val benchmark = new Benchmark(name, cardinality)

    benchmark.addCase(s"$name wholestage off", numIters = 2) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", value = false)
      f
    }

    benchmark.addCase(s"$name wholestage on", numIters = 5) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", value = true)
      f
    }

    benchmark.run()
  }

}

