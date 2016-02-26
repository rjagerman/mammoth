package ch.ethz.inf.da.mammoth.util

import java.util.concurrent.{Semaphore, TimeoutException}

import akka.util.Timeout
import breeze.linalg.{min, DenseVector}
import com.typesafe.scalalogging.slf4j.Logger
import glint.Client
import glint.models.client.BigVector
import org.slf4j.LoggerFactory

import scala.concurrent.{Promise, Future, ExecutionContext, Await}
import scala.concurrent.duration._
import spire.implicits._

/**
  * Creates an SSP clock
  *
  * @param partitionClock The clock
  * @param size The size
  */
class SSPClock(val partitionClock: BigVector[Int],
               val size: Int)(@transient implicit val ec: ExecutionContext) extends Serializable {

  /**
    * Blocks execution until iteration t has globally finished
    *
    * @param t The iteration
    */
  def block(t: Int)(implicit ec: ExecutionContext, timeout: Timeout): Unit = {
    var finished = false
    while (!finished) {
      try {
        finished = Await.result(hasGloballyFinished(t), timeout.duration)
      }
      if (!finished) {
        Thread.sleep(5000)
      }
    }
  }

  /**
    * Increments the clock for given partition by one
    *
    * @param partition The partition for which to increment the clock
    */
  def tick(partition: Int)(implicit ec: ExecutionContext, timeout: Timeout): Unit = {
    Await.result(partitionClock.push(Array(partition), Array(1)), timeout.duration)
  }

  /**
    * Checks if given iteration t has globally finished
    *
    * @param t The iteration to have finished
    * @return True if the iteration has globally finished, false otherwise
    */
  private def hasGloballyFinished(t: Int)(implicit ec: ExecutionContext, timeout: Timeout): Future[Boolean] = {
    partitionClock.pull((0L until size).toArray).map { case clockValues =>
      min(DenseVector(clockValues)) >= t
    }
  }

  /**
    * Destroys the SSP clock and releases the resources on the parameter servers
    *
    * @return A future containing the success or failure to delete the SSP clock
    */
  def destroy()(implicit ec: ExecutionContext, timeout: Timeout): Future[Boolean] = {
    partitionClock.destroy()
  }

}

object SSPClock {
  def apply(client: Client, size: Int)(implicit ec: ExecutionContext): SSPClock = {
    new SSPClock(client.vector[Int](size), size)
  }
}
