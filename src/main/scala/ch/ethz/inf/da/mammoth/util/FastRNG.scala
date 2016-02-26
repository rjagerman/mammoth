package ch.ethz.inf.da.mammoth.util

/**
  * XORShift random number generator.
  * This is a highly optimized random number generator that relies on the bitwise representation of numbers in the JVM.
  *
  * @param seed The random number generator's seed
  */
class FastRNG(seed: Long) {

  // Internal state of the RNG
  var x: Long = seed
  var y: Int = seed.toInt

  // This is just (1.0 / Int.MAX_VALUE) - epsilon, which allows fast multiplication instead of slow division
  // for generating doubles in range [0, 1)
  private final val OneOverMaxValue: Double = 4.656612875245796e-10

  /**
    * Generates the next long value
    *
    * @return The next long
    */
  @inline
  def nextLong(): Long = {
    x ^= (x << 21)
    x ^= (x >>> 35)
    x ^= (x << 4)
    x
  }

  /**
    * Generates the next positive long value
    *
    * @return A random long guaranteed to be positive
    */
  @inline
  def nextPositiveLong(): Long = {
    nextLong() >>> 1
  }

  /**
    * Generates the next random integer
    *
    * @return A random integer
    */
  @inline
  def nextInt(): Int = {
    y ^= (y << 13)
    y ^= (y >>> 17)
    y ^= (y << 5)
    y
  }

  /**
    * Generates the next random positive integer
    *
    * @return A random integer
    */
  @inline
  def nextPositiveInt(): Int = {
    nextInt() >>> 1
  }

  /**
    * Generates the next random double between [0, 1)
    *
    * @return A random double between [0, 1)
    */
  @inline
  def nextDouble(): Double = {
    nextPositiveInt() * OneOverMaxValue
  }

  /**
    * Flips a biased coin with probability p
    *
    * @param p The probability
    * @return True with probability p, false with probability (1-p)
    */
  @inline
  def coinflip(p: Double): Boolean = {
    nextPositiveLong() < (Long.MaxValue * p).toLong
  }

}
