package ch.ethz.inf.da.mammoth.util

import scala.concurrent.Future

/**
  * An iterator for pipelining futures (prefetching next results)
  *
  * @param getFuture A function that maps an integer index to a future
  * @param size The total number of futures to produce
  * @tparam T The type of futures
  */
class PipelinedFutureIterator[T](getFuture: Int => Future[T], size: Int) extends Iterator[Future[T]] {

  var currentIndex: Int = 0
  var nextFuture: Future[T] = getFuture(0)

  override def hasNext = currentIndex <= size

  override def next() = {
    val result = nextFuture
    currentIndex += 1
    if (currentIndex < size) {
      nextFuture = getFuture(currentIndex)
    }
    result
  }

}
