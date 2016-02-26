package ch.ethz.inf.da.mammoth.lda

/**
  * Configuration for the LDA solver
  *
  * @param α Prior on the document-topic distributions
  * @param β Prior on the topic-word distributions
  * @param iterations The number of iterations
  * @param topics The number of topics
  * @param vocabularyTerms The number of vocabulary terms
  * @param partitions The number of partitions
  * @param blockSize The size of a block of coordinates to process at a time
  * @param seed The random seed
  */
case class LDAConfig(var α: Double = 0.5,
                     var β: Double = 0.01,
                     var τ: Int = 1,
                     var iterations: Int = 100,
                     var topics: Int = 10,
                     var vocabularyTerms: Int = 100000,
                     var partitions: Int = 240,
                     var blockSize: Int = 10,
                     var seed: Int = 42)
