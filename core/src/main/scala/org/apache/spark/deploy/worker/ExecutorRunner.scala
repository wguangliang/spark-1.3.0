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

package org.apache.spark.deploy.worker

import java.io._

import scala.collection.JavaConversions._

import akka.actor.ActorRef
import com.google.common.base.Charsets.UTF_8
import com.google.common.io.Files

import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages.ExecutorStateChanged
import org.apache.spark.util.logging.FileAppender

/**
 * Manages the execution of one executor process.
 * This is currently only used in standalone mode.
 */
private[spark] class ExecutorRunner(
    val appId: String,
    val execId: Int,
    val appDesc: ApplicationDescription,
    val cores: Int,
    val memory: Int,
    val worker: ActorRef,
    val workerId: String,
    val host: String,
    val webUiPort: Int,
    val publicAddress: String,
    val sparkHome: File,
    val executorDir: File,
    val workerUrl: String,
    val conf: SparkConf,
    val appLocalDirs: Seq[String],
    var state: ExecutorState.Value)
  extends Logging {

  val fullId = appId + "/" + execId
  var workerThread: Thread = null
  var process: Process = null
  var stdoutAppender: FileAppender = null
  var stderrAppender: FileAppender = null

  // NOTE: This is now redundant with the automated shut-down enforced by the Executor. It might
  // make sense to remove this in the future.
  var shutdownHook: Thread = null
  // ExecutorRunner对象的start方法，在worker的case LaunchExecutor 382行被调用
  def start() {
    // 先创建一个线程对象，然后覆写run方法，通过一个线程来启动一个java子进程
    // 如果直接创建一个java子进程，可能会由于启动进程过慢和运行时间过长带来阻塞堵塞worker时间过长的问题
    workerThread = new Thread("ExecutorRunner for " + fullId) {
      override def run() { fetchAndRunExecutor() } // 创建Executor进程
    }
    // 调用线程对象的start方法，调用该线程对象的run方法
    workerThread.start()
    // Shutdown hook that kills actors on shutdown.
    shutdownHook = new Thread() {
      override def run() {
        killProcess(Some("Worker shutting down"))
      }
    }
    Runtime.getRuntime.addShutdownHook(shutdownHook)
  }

  /**
   * Kill executor process, wait for exit and notify worker to update resource status.
   *
   * @param message the exception message which caused the executor's death
   */
  private def killProcess(message: Option[String]) {
    var exitCode: Option[Int] = None
    if (process != null) {
      logInfo("Killing process!")
      process.destroy()
      process.waitFor()
      if (stdoutAppender != null) {
        stdoutAppender.stop()
      }
      if (stderrAppender != null) {
        stderrAppender.stop()
      }
      exitCode = Some(process.waitFor())
    }
    worker ! ExecutorStateChanged(appId, execId, state, message, exitCode)
  }

  /** Stop this executor runner, including killing the process it launched */
  def kill() {
    if (workerThread != null) {
      // the workerThread will kill the child process when interrupted
      workerThread.interrupt()
      workerThread = null
      state = ExecutorState.KILLED
      try {
        Runtime.getRuntime.removeShutdownHook(shutdownHook)
      } catch {
        case e: IllegalStateException => None
      }
    }
  }

  /** Replace variables such as {{EXECUTOR_ID}} and {{CORES}} in a command argument passed to us */
  def substituteVariables(argument: String): String = argument match {
    case "{{WORKER_URL}}" => workerUrl
    case "{{EXECUTOR_ID}}" => execId.toString
    case "{{HOSTNAME}}" => host
    case "{{CORES}}" => cores.toString
    case "{{APP_ID}}" => appId
    case other => other
  }

  /**
   * Download and run the executor described in our ApplicationDescription
   */
  // 线程对象调用该方法来启动java子进程，该子进程为Executor，即CoarseGrainedExecutorBackend
  def fetchAndRunExecutor() {
    try {
      // Launch the process
      // 启动子进程，使用java的ProcessBuilder来创建
      // 将运行参数传进去创建ProcessBuilder
      val builder = CommandUtils.buildProcessBuilder(appDesc.command, memory,
        sparkHome.getAbsolutePath, substituteVariables)  // ProcessBuilder
      val command = builder.command()
      logInfo("Launch command: " + command.mkString("\"", "\" \"", "\""))

      builder.directory(executorDir) // 更改工作目录到为executor创建的目录
      builder.environment.put("SPARK_LOCAL_DIRS", appLocalDirs.mkString(","))
      // In case we are running this from within the Spark Shell, avoid creating a "scala"
      // parent process for the executor command
      builder.environment.put("SPARK_LAUNCH_WITH_SCALA", "0")

      // Add webUI log urls
      val baseUrl =
        s"http://$publicAddress:$webUiPort/logPage/?appId=$appId&executorId=$execId&logType="
      builder.environment.put("SPARK_LOG_URL_STDERR", s"${baseUrl}stderr")
      builder.environment.put("SPARK_LOG_URL_STDOUT", s"${baseUrl}stdout")
      // 真正启动一个子进程，该子进程为Executor，即CoarseGrainedExecutorBackend
      process = builder.start()  // 启动CoarseGrainedExecutorBackend进程，下面进入 CoarseGrainedExecutorBackend类的main方法
      val header = "Spark Executor Command: %s\n%s\n\n".format(
        command.mkString("\"", "\" \"", "\""), "=" * 40)

      // Redirect its stdout and stderr to files
      val stdout = new File(executorDir, "stdout")
      stdoutAppender = FileAppender(process.getInputStream, stdout, conf) // 进程标准输出

      val stderr = new File(executorDir, "stderr")
      Files.write(header, stderr, UTF_8)
      stderrAppender = FileAppender(process.getErrorStream, stderr, conf) // 进程错误输出

      // Wait for it to exit; executor may exit with code 0 (when driver instructs it to shutdown)
      // or with nonzero exit code
      // 等待结束。如果driver通知结束，则会返回0。否则的话非正常退出，返回非0
      val exitCode = process.waitFor() // 当前线程阻塞等待该进程的结束
      state = ExecutorState.EXITED // 该进程执行结束了，才会走到这句代码
      val message = "Command exited with code " + exitCode
      // 向worker发送Executor状态改变的消息，Executor进程执行结束了
      worker ! ExecutorStateChanged(appId, execId, state, Some(message), Some(exitCode))
    } catch {
      case interrupted: InterruptedException => {
        logInfo("Runner thread for executor " + fullId + " interrupted")
        state = ExecutorState.KILLED
        killProcess(None)
      }
      case e: Exception => {
        logError("Error running executor", e)
        state = ExecutorState.FAILED
        killProcess(Some(e.toString))
      }
    }
  }
}
