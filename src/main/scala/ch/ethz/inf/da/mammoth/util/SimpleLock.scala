package ch.ethz.inf.da.mammoth.util

import java.util.concurrent.{TimeUnit, Semaphore}

import com.typesafe.scalalogging.slf4j.Logger

/**
  * A simple lock for a limited multiple simultaneous accesses (e.g. a Semaphore)
  *
  * @param accesses Number of simultaneous accesses that are allowed
  */
class SimpleLock(val accesses: Int, var logger: Logger = null) {

  val semaphore = new Semaphore(accesses)
  var waitTime: Long = 0

  /**
    * Acquires a single lock
    */
  @inline
  def acquire(): Unit = {
    val start = System.currentTimeMillis()
    while(!semaphore.tryAcquire(10, TimeUnit.SECONDS)) {
      if (logger != null) {
        logger.info(s"Failed to acquire lock, retrying...")
      }
    }
    waitTime += System.currentTimeMillis() - start
  }

  /**
    * Releases a single lock
    */
  @inline
  def release(): Unit = {
    semaphore.release()
  }

  /**
    * Acquires multiple locks
    * @param accesses The amount of locks
    */
  @inline
  def acquire(accesses: Int): Unit = {
    val start = System.currentTimeMillis()
    while(!semaphore.tryAcquire(accesses, 10, TimeUnit.SECONDS)) {
      if (logger != null) {
        logger.info(s"Failed to acquire lock, retrying...")
      }
    }
    waitTime += System.currentTimeMillis() - start
  }

  /**
    * Releases multiple locks
    * @param accesses The amount of locks
    */
  @inline
  def release(accesses: Int): Unit = {
    semaphore.release(accesses)
  }

  /**
    * Acquires all locks
    */
  @inline
  def acquireAll(): Unit = {
    val start = System.currentTimeMillis()
    while(!semaphore.tryAcquire(accesses, 10, TimeUnit.SECONDS)) {
      if (logger != null) {
        logger.info(s"Failed to acquire lock, retrying...")
      }
    }
    waitTime += System.currentTimeMillis() - start
  }

  /**
    * Releases all locks
    */
  @inline
  def releaseAll(): Unit = {
    semaphore.release(accesses)
  }

}
