package ch.ethz.inf.da.mammoth.util

import java.util.concurrent.{Semaphore, TimeoutException}

import akka.util.Timeout
import breeze.linalg.{min, DenseVector}
import com.typesafe.scalalogging.slf4j.Logger
import glint.Client
import org.slf4j.LoggerFactory

import scala.concurrent.{Promise, Future, ExecutionContext, Await}
import scala.concurrent.duration._
import spire.implicits._

/**
  * Creates an SSP clock
  *
  * @param gc The glint client
  * @param size The size (number of partitions) of the SSP clock
  */
class SSPClock(@transient val gc: Client,
               val size: Int)(@transient implicit val ec: ExecutionContext) extends Serializable {

  val vector = gc.vector[Int](size)

  private implicit val timeout: Timeout = new Timeout(120 seconds)

  @transient
  protected lazy val logger: Logger = Logger(LoggerFactory getLogger getClass.getName)

  /**
    * Blocks execution until iteration t has globally finished
    *
    * @param t The iteration
    */
  def block(t: Int)(implicit ec: ExecutionContext): Unit = {
    var finished = false
    while (!finished) {
      try {
        finished = Await.result(hasGloballyFinished(t), timeout.duration)
      } catch {
        case ex: Exception => logger.warn(ex.getMessage)
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
  def tick(partition: Int)(implicit ec: ExecutionContext): Unit = {
    Await.result(vector.push(Array(partition), Array(1)), timeout.duration)
  }

  /**
    * Checks if given iteration t has globally finished
    *
    * @param t The iteration to have finished
    * @return True if the iteration has globally finished, false otherwise
    */
  private def hasGloballyFinished(t: Int)(implicit ec: ExecutionContext): Future[Boolean] = {
    vector.pull((0L until size).toArray).map { case clockValues =>
      min(DenseVector(clockValues)) >= t
    }
  }

  /**
    * Destroys the SSP clock and releases the resources on the parameter servers
    *
    * @return A future containing the success or failure to delete the SSP clock
    */
  def destroy()(implicit ec: ExecutionContext): Future[Boolean] = {
    vector.destroy()
  }

}

object SSPClock {
  def apply(client: Client, size: Int)(implicit ec: ExecutionContext): SSPClock = {
    new SSPClock(client, size)
  }
}
