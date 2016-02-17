package ch.ethz.inf.da.mammoth

import java.net.URI

import com.typesafe.scalalogging.slf4j.Logger
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spire.implicits.cfor

import scala.collection.immutable.NumericRange.Exclusive
import scala.util.Random

/**
  * Utility functions
  */
package object util {

  /**
    * Encodes a pair of integers in the first and last 32 bits of a long
    *
    * @param pair The pair of integers to encode
    * @return The long
    */
  def intPairToLong(pair: (Int, Int)): Long = (pair._1.toLong << 32) + pair._2.toLong

  /**
    * Decodes a long to a pair of integers using the first and last 32 bits
    *
    * @param long The long to decode
    * @return The pair of integers
    */
  def longToIntPair(long: Long): (Int, Int) = ((long >> 32).toInt, long.toInt)

  /**
    * Simple function to log time since something happened
    *
    * @param start The start time
    * @param logger The logger to log to
    * @param message The message to prepend
    */
  def timeSince(start: Long, logger: Logger, message: String = ""): Unit = {
    val elapsed = System.currentTimeMillis() - start
    logger.info(s"$message${elapsed}ms")
  }

  /**
    * Constructs an array with elements [0, ..., n-1] shuffled in a random order
    *
    * @param random The random number generator
    * @param n The number of elements
    * @return An array with elements [0, ..., n-1] shuffled in a random order
    */
  def shuffledRange(random: FastRNG, n: Int): Array[Int] = {
    val array = new Array[Int](n)
    cfor(0)(_ < n, _ + 1)(i => {
      array(i) = i
    })
    cfor(0)(_ < n - 2, _ + 1)(i => {
      val j = random.nextPositiveInt() % (n - i)
      val tmp = array(i)
      array(i) = array(j)
      array(j) = tmp
    })
    array
  }

  /**
    * Simple iterative loop using while to translate into fast JVM bytecode
    *
    * @param start The start index
    * @param end The end index
    * @param step The increment step size
    * @param function The function to execute on each iteration
    */
  def loop(start: Int, end: Int, step: Int)(@inline function: Int => Unit): Unit = {
    var i = start
    if (end > start) {
      while (i < end) {
        function(i)
        i += step
      }
    } else {
      while (i > end) {
        function(i)
        i += step
      }
    }
  }

  /**
    * Provides easy to use RDD improvements
    *
    * @param rdd The RDD to function on
    * @tparam A The type of elements
    */
  implicit class RDDImprovements[A](rdd: RDD[A]) {

    /**
      * Executes a function for each partition within the RDD and provides an integer partition index
      *
      * @param func The function to execute on each partition
      */
    def foreachPartitionWithIndex(func: (Int, Iterator[A]) => Unit): Unit = {
      rdd.mapPartitionsWithIndex {
        case (index, elements) =>
          func(index, elements)
          Iterator.single[Int](0)
      }.foreachPartition {
        case _ => ()
      }
    }

  }

  /**
    * Generate (non-overlapping) ranges covering [start, end) with approximate size of given blocksize
    *
    * @param start The start index
    * @param end The end index
    * @param blockSize The approximate index
    * @param bounded If set to true it will guarantee any generated range is smaller than blockSize
    * @return The (non-overlapping) ranges as an iterator
    */
  def ranges(start: Long, end: Long, blockSize: Long, bounded: Boolean = true): Iterator[Exclusive[Long]] = {
    val size = end - start
    val approxSplits = size.toDouble / blockSize.toDouble
    val splits = if (bounded) Math.ceil(approxSplits).toInt else Math.round(approxSplits).toInt

    (0 until splits).map {
      case index =>
        val rangeStart = start + Math.ceil(index * (size.toDouble / splits.toDouble)).toLong
        val rangeEnd = start + Math.ceil((index + 1) * (size.toDouble / splits.toDouble)).toLong
        rangeStart until rangeEnd
    }.toIterator
  }

  /**
    * Checks in a generic sense if given file exists (either on HDFS or local file system)
    *
    * @param sc The spark context
    * @param file The file
    * @return True if the file exists, false otherwise
    */
  def fileExists(sc: SparkContext, file: String): Boolean = {
    file match {
      case "" => false

      case f if f.startsWith("hdfs://") =>
        val uri = URI.create(f)
        val fs = FileSystem.get(uri, sc.hadoopConfiguration)
        fs.exists(new Path(f))

      case f =>
        new java.io.File(f).exists
    }
  }

  def time[A](logger: Logger, pre: String)(a: => A): A = {
    val start = System.currentTimeMillis()
    val result = a
    val elapsed = System.currentTimeMillis() - start
    logger.info(s"${pre}${elapsed}ms")
    result
  }

}
