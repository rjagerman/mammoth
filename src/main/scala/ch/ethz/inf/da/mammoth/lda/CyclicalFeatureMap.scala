package ch.ethz.inf.da.mammoth.lda

/**
  * Remaps feature indices to balance according to power-law distribution of words
  *
  * @param features The total number of features
  * @param partitions The number of partitions to cyclicly map over
  */
class CyclicalFeatureMap(features: Int, partitions: Int) extends Serializable {

  val map: Array[Int] = computeCyclicalFeatureMap()

  /**
    * Computes the cyclical feature map
    *
    * @return The cyclical feature map
    */
  private def computeCyclicalFeatureMap(): Array[Int] = {
    val result = new Array[Int](features)
    var i = 0
    while (i < features) {
      result(i) = remap(i)
      i += 1
    }
    result
  }

  /**
    * Maps a feature index to a new feature index according to a cyclic scheme
    *
    * @param feature The feature
    * @return The remapped feature
    */
  @inline
  private def remap(feature: Int): Int = {
    val partition = feature % partitions
    val indexInPartition = (feature - partition) / partitions

    // Due to the cyclic nature of the features, the first few partitions will obtain more features than the rest
    // To model this accurately we first compute how many bigger partitions there will be and then we compute the
    // bigger partition size and the smaller partition size (these should always differ by 1)
    val biggerPartitions = features % partitions
    val smallerPartitionSize = features / partitions
    val biggerPartitionSize = smallerPartitionSize + 1

    if (partition < biggerPartitions) {
      // If our selected partition is in the first set of partitions (the bigger one), we use the bigger partition size
      // to compute its offset
      indexInPartition + partition * biggerPartitionSize
    } else {
      // Otherwise, it is in the later set of partitions and we need to add all the bigger size partitions and only a
      // few smaller partitions to it to find the correct offset
      indexInPartition + (partition - biggerPartitions) * smallerPartitionSize + (biggerPartitions * biggerPartitionSize)
    }
  }

}