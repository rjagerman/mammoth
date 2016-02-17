package ch.ethz.inf.da.mammoth.util

import java.util.PriorityQueue

import scala.collection.JavaConverters._

/**
  * A bounded priority queue
  *
  * @param max The maximum amount of elements to store
  * @param ord The implicit ordering of the elements
  * @tparam A The type of elements to store
  */
class BoundedPriorityQueue[A](max: Int)(implicit ord: Ordering[A]) extends Iterable[A] with Serializable {

  private val q = new PriorityQueue[A](max, ord)

  override def size: Int = q.size()

  def enqueue(elem: A): Unit = {
    if (size < max) {
      q.offer(elem)
    } else {
      val head = q.peek()
      if (head != null && ord.gt(elem, head)) {
        q.poll()
        q.offer(elem)
      }
    }
  }

  def clear(): Unit = q.clear()

  override def iterator: Iterator[A] = q.iterator.asScala

}
