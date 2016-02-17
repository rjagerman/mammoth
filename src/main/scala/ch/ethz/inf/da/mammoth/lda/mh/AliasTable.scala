package ch.ethz.inf.da.mammoth.lda.mh

import breeze.linalg.{Vector, sum}
import ch.ethz.inf.da.mammoth.util.FastRNG

/**
  * An alias table for quickly drawing probabilities from an uneven distribution using Vose's method:
  * Vose, Michael D. "A linear algorithm for generating random numbers with a given distribution." 1991
  *
  * @param alias The aliases
  * @param prob The probabilities
  */
class AliasTable(alias: Array[Int], prob: Array[Double]) {

  var count: Int = 0

  /**
    * Creates a new alias table for given vector of counts (interpreted as scaled probabilities)
    *
    * @param counts The counts
    */
  def this(counts: Vector[Double]) {
    this(new Array[Int](counts.length), new Array[Double](counts.length))

    val n = counts.length
    val countSum = sum(counts)

    val small = new Array[Int](n)
    val large = new Array[Int](n)

    var l = 0
    var s = 0

    val p = new Array[Double](n)
    var i = 0
    while (i < n) {
      val pi = n * (counts(i) / countSum)
      p(i) = pi
      if (pi < 1.0) {
        small(s) = i
        s += 1
      } else {
        large(l) = i
        l += 1
      }
      i += 1
    }

    while (s != 0 && l != 0) {
      s = s - 1
      l = l - 1
      val j = small(s)
      val k = large(l)
      prob(j) = p(j)
      alias(j) = k
      p(k) = (p(k) + p(j)) - 1
      if (p(k) < 1.0) {
        small(s) = k
        s += 1
      } else {
        large(l) = k
        l += 1
      }
    }
    while (s > 0) {
      s -= 1
      prob(small(s)) = 1.0
    }
    while (l > 0) {
      l -= 1
      prob(large(l)) = 1.0
    }
  }

  /**
    * Draws from the uneven multinomial probability distribution represented by this Alias table
    *
    * @param random The random number generator to use
    * @return A random outcome from the uneven multinomial probability distribution
    */
  def draw(random: FastRNG): Int = {
    count += 1
    val i = random.nextPositiveInt() % alias.length
    if (random.coinflip(prob(i))) {
      i
    } else {
      alias(i)
    }
  }

}
