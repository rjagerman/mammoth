package ch.ethz.inf.da.mammoth.topicmodeling

import breeze.linalg._

/**
 * Computes an LDA model on given data using an EM algorithm
 *
 * @param iterations The number of iterations
 * @param data The data as an array of sparse feature vector
 * @param β The initial topic model
 */
class LDASolver(iterations:Int,
                val data:Array[SparseVector[Double]],
                val β:DenseMatrix[Double]) extends EMSolver(iterations) {

  val topics = β.cols
  val features = β.rows

  /**
   * Sparse latent structure π
   * Indexed by i,k,j:
   *   i = Document index
   *   k = Topic index
   *   j = Word index
   */
  val π = Array.fill[Array[scala.collection.mutable.HashMap[Int, Double]]](data.length) {
    Array.fill[scala.collection.mutable.HashMap[Int, Double]](topics) {
      new scala.collection.mutable.HashMap[Int, Double]()
    }
  }

  // Better code would use tuples to index a single hash map, but this causes memory explosions due to boxing of tuples:
  // val π = scala.collection.mutable.HashMap[(Int, Int, Int), Double]()

  /**
   * Document-topic distributions θ
   * These are initialized using the topic-word distributions in given topic model.
   * Indexed by i,k:
   *   i = Document index
   *   k = Topic index
   */
  val θ = DenseMatrix.zeros[Double](data.length, topics)
  for (k <- 0 until topics; i <- data.indices) {
    θ(i, k) = data(i).dot(β(::, k))
  }

  /**
   * Performs a single EStep.
   */
  override def EStep(): Unit = {

    // Iterate over all documents i (in this partition)
    for (i <- data.indices) {

      // Iterate over non-zero features j of document i
      this.data(i).activeKeysIterator.foreach { case j =>

        var sum: Double = 0.0

        // Compute π_{i,j,k} while keeping track of the sum over all k
        for (k <- 0 until topics) {
          π(i)(k)(j) = θ(i, k) * β(j, k)
          sum += π(i)(k)(j)
        }

        // Normalize π_{i,j,k} by dividing it by the sum
        for (k <- 0 until topics) {
          if (sum != 0) {
            π(i)(k)(j) /= sum
          }
        }
      }
    }

  }

  /**
   * Performs a single MStep.
   */
  override def MStep(): Unit = {
    this.MStepθ()
    this.MStepβ()
  }

  /**
   * Computes θ_{i,k} as part of the M-Step
   */
  def MStepθ(): Unit = {

    // Iterate over all documents i (in this partition)
    for (i <- data.indices) {

      // Compute 1.0 / C_i
      val C_i = 1.0 / data(i).activeValuesIterator.sum

      // Iterate over all topics k
      for (k <- 0 until topics) {

        // Compute θ_{i,k}
        θ(i, k) = C_i * data(i).activeIterator.map {
          case (j, value) => π(i)(k)(j) * value
        }.sum

      }

    }
  }

  /**
   * Computes β_{j,k} as part of the M-Step
   */
  def MStepβ(): Unit = {

    // Iterate over all topics k
    for (k <- 0 until topics) {

      // Compute 1.0 / C_k
      val C_k = 1.0 / data.indices.map { i =>
        data(i).activeIterator.map {
          case (j, value) => π(i)(k)(j) * value
        }.sum
      }.sum

      // Compute β_{j,k} by constructing a new vector β_{_,k} for all j in one go
      β(::, k) := DenseVector.zeros[Double](features)
      data.indices.foreach {
        i => data(i).activeIterator.foreach {
          case (j, value) => β(j, k) = β(j, k) + value * π(i)(k).getOrElse(j, 0.0)
        }
      }

      // Normalize by multiplying C_k
      β(::, k) :*= C_k

    }

  }

}
