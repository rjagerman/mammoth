package ch.ethz.inf.da.mammoth.lda

import breeze.linalg._

/**
 * Solves LDA on given data using an EM algorithm
 *
 * @param iterations The number of iterations
 * @param data The data as an array of sparse feature vector
 * @param model The LDA model to start with
 */
class LDASolver(iterations:Int,
                val data:Array[SparseVector[Double]],
                val model:LDAModel) extends EMSolver(iterations) {

  /**
   * Sparse latent structure π
   * Indexed by i,j,k:
   *   i = Document index
   *   j = Word index
   *   k = Topic index
   */
  val π = scala.collection.mutable.HashMap[(Int, Int, Int), Double]()

  /**
   * Document-topic distributions θ
   * These are initialized using the topic-word distributions in given LDA model.
   * Indexed by i,k:
   *   i = Document index
   *   k = Topic index
   */
  val θ = DenseMatrix.zeros[Double](data.length, model.topics)
  (0 until model.topics).foreach (
    k => data.indices.foreach (
      i => θ(i,k) = data(i).dot(model.β(::, k))
    )
  )

  /**
   * Set the model's number of documents to the size of the data we will train on
   */
  model.documents = data.length

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
        for (k <- 0 until model.topics) {
          π((i, j, k)) = θ(i, k) * model.β(j, k)
          sum += π((i, j, k))
        }

        // Normalize π_{i,j,k} by dividing it by the sum
        for (k <- 0 until model.topics) {
          π((i, j, k)) /= sum
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
      for (k <- 0 until model.topics) {

        // Compute θ_{i,k}
        θ(i, k) = C_i * data(i).activeIterator.map {
          case (j, value) => π((i,j,k)) * value
        }.sum

      }

    }
  }

  /**
   * Computes β_{j,k} as part of the M-Step
   */
  def MStepβ(): Unit = {

    // Iterate over all topics k
    for (k <- 0 until model.topics) {

      // Compute 1.0 / C_k
      val C_k = 1.0 / data.indices.map { i =>
        data(i).activeIterator.map {
          case (j, value) => π((i,j,k)) * value
        }.sum
      }.sum

      // Compute β_{j,k} by constructing a new vector β_{_,k} for all j in one go
      model.β(::, k) := DenseVector.zeros[Double](model.features)
      data.indices.foreach {
        i => data(i).activeIterator.foreach {
          case (j, value) => model.β(j, k) = model.β(j, k) + value * π.getOrElse((i,j,k), 0.0)
        }
      }

      // Normalize by multiplying C_k
      model.β(::, k) :*= C_k

    }

  }

}
