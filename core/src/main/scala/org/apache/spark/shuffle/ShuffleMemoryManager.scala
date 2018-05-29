/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle

import scala.collection.mutable

import org.apache.spark.{Logging, SparkException, SparkConf}

/**
 * Allocates a pool of memory to task threads for use in shuffle operations. Each disk-spilling
 * collection (ExternalAppendOnlyMap or ExternalSorter) used by these tasks can acquire memory
 * from this pool and release it as it spills data out. When a task ends, all its memory will be
 * released by the Executor.
 *
 * This class tries to ensure that each thread gets a reasonable share of memory, instead of some
 * thread ramping up to a large amount first and then causing others to spill to disk repeatedly.
 * If there are N threads, it ensures that each thread can acquire at least 1 / 2N of the memory
 * before it has to spill, and at most 1 / N. Because N varies dynamically, we keep track of the
 * set of active threads and redo the calculations of 1 / 2N and 1 / N in waiting threads whenever
 * this set changes. This is all done by synchronizing access on "this" to mutate state and using
 * wait() and notifyAll() to signal changes.
 * 负责管理shuffle线程占用内存的分配和释放，并通过threadMemory:mutable.HashMap[Long, Long]缓存每个线程的内存字节数
 * 在一个executor中可以并行执行多个task，这些task都可能发生shuffle，
 * 每个task看作一个线程，这些线程公用一个内存池，这时就涉及到内存的使用策略了，
 * 申请过多会导致其他task spill内存不足，过少又会影响自身效率，spark中对这块的内存管理位于ShuffleMemoryManager类中，
 * 基本的分配策略是如果线程数为n，那么spark可以确保一个线程的内存在1/n和1/2n之间，
 * 由于线程数是动态的，因此计算也是动态的，当一个task使用完后，executor负责release此内存。内存的申请和释放在spill时产生。
 */
private[spark] class ShuffleMemoryManager(maxMemory: Long) extends Logging {

  // maxMemory = (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  // 缓存每个线程的内存字节数
  private val threadMemory = new mutable.HashMap[Long, Long]()  // threadId -> memory bytes

  def this(conf: SparkConf) = this(ShuffleMemoryManager.getMaxMemory(conf))

  /**
   * Try to acquire up to numBytes memory for the current thread, and return the number of bytes
   * obtained, or 0 if none can be allocated. This call may block until there is enough free memory
   * in some situations, to make sure each thread has a chance to ramp up to at least 1 / 2N of the
   * total memory pool (where N is the # of active threads) before it is forced to spill. This can
   * happen if the number of threads increases but an older thread had a lot of memory already.
   */
  // 尝试获得内存方法
  // 此方法用于当前线程尝试获得numBytes大小的内存，并返回实际获得的内存大小。
  //   处理逻辑：
  //      假设当前有N个线程，必须保证每个线程在溢出之前至少获得 1/(2N) 的内存，并且每个线程最多获得 1/N 的内存。
  //      由于N是动态变化的变量，所以要持续对这些线程进行跟踪，以便无论何时在这些线程变化时重新按照1/(2N) 和 1/N 计算
  def tryToAcquire(numBytes: Long): Long = synchronized {
    val threadId = Thread.currentThread().getId // 当前线程id
    assert(numBytes > 0, "invalid number of bytes requested: " + numBytes)

    // Add this thread to the threadMemory map just so we can keep an accurate count of the number
    // of active threads, to let other threads ramp down their memory in calls to tryToAcquire
    if (!threadMemory.contains(threadId)) {
      threadMemory(threadId) = 0L
      notifyAll()  // Will later cause waiting threads to wake up and check numThreads again
    }

    // Keep looping until we're either sure that we don't want to grant this request (because this
    // thread would have more than 1 / numActiveThreads of the memory) or we have enough free
    // memory to give it (we always let each thread get at least 1 / (2 * numActiveThreads)).
    while (true) {
      val numActiveThreads = threadMemory.keys.size
      val curMem = threadMemory(threadId)
      val freeMemory = maxMemory - threadMemory.values.sum

      // How much we can grant this thread; don't let it grow to more than 1 / numActiveThreads;
      // don't let it be negative
      val maxToGrant = math.min(numBytes, math.max(0, (maxMemory / numActiveThreads) - curMem))

      if (curMem < maxMemory / (2 * numActiveThreads)) {
        // We want to let each thread get at least 1 / (2 * numActiveThreads) before blocking;
        // if we can't give it this much now, wait for other threads to free up memory
        // (this happens if older threads allocated lots of memory before N grew)
        if (freeMemory >= math.min(maxToGrant, maxMemory / (2 * numActiveThreads) - curMem)) {
          val toGrant = math.min(maxToGrant, freeMemory)
          threadMemory(threadId) += toGrant
          return toGrant
        } else {
          logInfo(s"Thread $threadId waiting for at least 1/2N of shuffle memory pool to be free")
          wait() // 等待其他线程释放内存
        }
      } else {
        // Only give it as much memory as is free, which might be none if it reached 1 / numThreads
        val toGrant = math.min(maxToGrant, freeMemory)
        threadMemory(threadId) += toGrant
        return toGrant
      }
    }
    0L  // Never reached
  }

  /** Release numBytes bytes for the current thread. */
  def release(numBytes: Long): Unit = synchronized {
    val threadId = Thread.currentThread().getId
    val curMem = threadMemory.getOrElse(threadId, 0L)
    if (curMem < numBytes) {
      throw new SparkException(
        s"Internal error: release called on ${numBytes} bytes but thread only has ${curMem}")
    }
    threadMemory(threadId) -= numBytes
    notifyAll()  // Notify waiters who locked "this" in tryToAcquire that memory has been freed
  }

  /** Release all memory for the current thread and mark it as inactive (e.g. when a task ends). */
  // 释放资源
  def releaseMemoryForThisThread(): Unit = synchronized {
    val threadId = Thread.currentThread().getId
    threadMemory.remove(threadId)
    notifyAll()  // Notify waiters who locked "this" in tryToAcquire that memory has been freed
  }
}

private object ShuffleMemoryManager {
  /**
   * Figure out the shuffle memory limit from a SparkConf. We currently have both a fraction
   * of the memory pool and a safety factor since collections can sometimes grow bigger than
   * the size we target before we estimate their sizes again.
   * //获取shuffle所有线程占用的最大内存
   *   shuffle所有线程占用的最大内存：Java运行时最大内存*Spark的shuffle最大内存占比*Spark的安全内存占比
   */
  def getMaxMemory(conf: SparkConf): Long = {
    val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)
    val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }
}
