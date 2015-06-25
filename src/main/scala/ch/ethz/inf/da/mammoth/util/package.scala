package ch.ethz.inf.da.mammoth

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

}
