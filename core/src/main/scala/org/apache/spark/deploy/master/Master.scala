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

package org.apache.spark.deploy.master

import java.io.FileNotFoundException
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

import akka.actor._
import akka.pattern.ask
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import org.apache.hadoop.fs.Path

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.{ApplicationDescription, DriverDescription,
  ExecutorState, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.master.MasterMessages._
import org.apache.spark.deploy.master.ui.MasterWebUI
import org.apache.spark.deploy.rest.StandaloneRestServer
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.scheduler.{EventLoggingListener, ReplayListenerBus}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, SignalLogger, Utils}

private[spark] class Master(
    host: String,
    port: Int,
    webUiPort: Int,
    val securityMgr: SecurityManager,
    val conf: SparkConf)
  extends Actor with ActorLogReceive with Logging with LeaderElectable {

  import context.dispatcher   // to use Akka's scheduler.schedule()

  val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)

  def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")  // For application IDs
  val WORKER_TIMEOUT = conf.getLong("spark.worker.timeout", 60) * 1000
  val RETAINED_APPLICATIONS = conf.getInt("spark.deploy.retainedApplications", 200)
  val RETAINED_DRIVERS = conf.getInt("spark.deploy.retainedDrivers", 200)
  val REAPER_ITERATIONS = conf.getInt("spark.dead.worker.persistence", 15)
  val RECOVERY_MODE = conf.get("spark.deploy.recoveryMode", "NONE")

  val workers = new HashSet[WorkerInfo]
  val idToWorker = new HashMap[String, WorkerInfo]
  val addressToWorker = new HashMap[Address, WorkerInfo]

  val apps = new HashSet[ApplicationInfo]
  val idToApp = new HashMap[String, ApplicationInfo]
  val actorToApp = new HashMap[ActorRef, ApplicationInfo]
  val addressToApp = new HashMap[Address, ApplicationInfo]
  val waitingApps = new ArrayBuffer[ApplicationInfo]
  val completedApps = new ArrayBuffer[ApplicationInfo]
  var nextAppNumber = 0
  val appIdToUI = new HashMap[String, SparkUI]

  val drivers = new HashSet[DriverInfo]
  val completedDrivers = new ArrayBuffer[DriverInfo]
  val waitingDrivers = new ArrayBuffer[DriverInfo] // Drivers currently spooled for scheduling
  var nextDriverNumber = 0

  Utils.checkHost(host, "Expected hostname")

  val masterMetricsSystem = MetricsSystem.createMetricsSystem("master", conf, securityMgr)
  val applicationMetricsSystem = MetricsSystem.createMetricsSystem("applications", conf,
    securityMgr)
  val masterSource = new MasterSource(this)

  val webUi = new MasterWebUI(this, webUiPort)

  val masterPublicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }

  val masterUrl = "spark://" + host + ":" + port
  var masterWebUiUrl: String = _

  var state = RecoveryState.STANDBY

  var persistenceEngine: PersistenceEngine = _

  var leaderElectionAgent: LeaderElectionAgent = _

  private var recoveryCompletionTask: Cancellable = _

  // As a temporary workaround before better ways of configuring memory, we allow users to set
  // a flag that will perform round-robin scheduling across the nodes (spreading out each app
  // among all the nodes) instead of trying to consolidate each app onto a small # of nodes.
  val spreadOutApps = conf.getBoolean("spark.deploy.spreadOut", true)

  // Default maxCores for applications that don't specify it (i.e. pass Int.MaxValue)
  val defaultCores = conf.getInt("spark.deploy.defaultCores", Int.MaxValue)
  if (defaultCores < 1) {
    throw new SparkException("spark.deploy.defaultCores must be positive")
  }

  // Alternative application submission gateway that is stable across Spark versions
  private val restServerEnabled = conf.getBoolean("spark.master.rest.enabled", true)
  private val restServer =
    if (restServerEnabled) {
      val port = conf.getInt("spark.master.rest.port", 6066)
      Some(new StandaloneRestServer(host, port, self, masterUrl, conf))
    } else {
      None
    }
  private val restServerBoundPort = restServer.map(_.start())

  override def preStart() {
    logInfo("Starting Spark master at " + masterUrl)
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    // Listen for remote client disconnection events, since they don't go through Akka's watch()
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    webUi.bind()
    masterWebUiUrl = "http://" + masterPublicAddress + ":" + webUi.boundPort
    //定时任务，用来检测超时的Worker节点
    context.system.scheduler.schedule(0 millis, WORKER_TIMEOUT millis, self, CheckForWorkerTimeOut)

    masterMetricsSystem.registerSource(masterSource)
    masterMetricsSystem.start()
    applicationMetricsSystem.start()
    // Attach the master and app metrics servlet handler to the web ui after the metrics systems are
    // started.
    masterMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    applicationMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)

    val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, SerializationExtension(context.system))
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
      case "FILESYSTEM" =>
        val fsFactory =
          new FileSystemRecoveryModeFactory(conf, SerializationExtension(context.system))
        (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))
      case "CUSTOM" =>
        val clazz = Class.forName(conf.get("spark.deploy.recoveryMode.factory"))
        val factory = clazz.getConstructor(conf.getClass, Serialization.getClass)
          .newInstance(conf, SerializationExtension(context.system))
          .asInstanceOf[StandaloneRecoveryModeFactory]
        (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
      case _ =>
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    super.preRestart(reason, message) // calls postStop()!
    logError("Master actor restarted due to exception", reason)
  }

  override def postStop() {
    masterMetricsSystem.report()
    applicationMetricsSystem.report()
    // prevent the CompleteRecovery message sending to restarted master
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel()
    }
    webUi.stop()
    restServer.foreach(_.stop())
    masterMetricsSystem.stop()
    applicationMetricsSystem.stop()
    persistenceEngine.close()
    leaderElectionAgent.stop()
  }

  override def electedLeader() {
    self ! ElectedLeader
  }

  override def revokedLeadership() {
    self ! RevokedLeadership
  }

  override def receiveWithLogging = { //其实就是Actor.Receive方法
    case ElectedLeader => {
      val (storedApps, storedDrivers, storedWorkers) = persistenceEngine.readPersistedData()
      state = if (storedApps.isEmpty && storedDrivers.isEmpty && storedWorkers.isEmpty) {
        RecoveryState.ALIVE
      } else {
        RecoveryState.RECOVERING
      }
      logInfo("I have been elected leader! New state: " + state)
      if (state == RecoveryState.RECOVERING) {
        beginRecovery(storedApps, storedDrivers, storedWorkers)
        recoveryCompletionTask = context.system.scheduler.scheduleOnce(WORKER_TIMEOUT millis, self,
          CompleteRecovery)
      }
    }

    case CompleteRecovery => completeRecovery()

    case RevokedLeadership => {
      logError("Leadership has been revoked -- master shutting down.")
      System.exit(0)
    }
    //当接收到Worker发过来的worker节点信息
    case RegisterWorker(id, workerHost, workerPort, cores, memory, workerUiPort, publicAddress) =>
    {
      logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
        workerHost, workerPort, cores, Utils.megabytesToString(memory)))
      if (state == RecoveryState.STANDBY) { //如果该master是standby状态，忽略
        // ignore, don't send response
      } else if (idToWorker.contains(id)) { //如果已经注册的worker队列里存在
        sender ! RegisterWorkerFailed("Duplicate worker ID")
      } else { //进行注册
        val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory,
          sender, workerUiPort, publicAddress)
        if (registerWorker(worker)) {
          persistenceEngine.addWorker(worker) //持久化引擎，持久化worker信息
          sender ! RegisteredWorker(masterUrl, masterWebUiUrl) //master向worker发送注册成功的信息（master的url，master的webui的url）
          schedule()  //资源的调度
        } else {
          val workerAddress = worker.actor.path.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          sender ! RegisterWorkerFailed("Attempted to re-register worker at same address: "
            + workerAddress)
        }
      }
    }

    case RequestSubmitDriver(description) => {
      if (state != RecoveryState.ALIVE) {
        val msg = s"Can only accept driver submissions in ALIVE state. Current state: $state."
        sender ! SubmitDriverResponse(false, None, msg)
      } else {
        logInfo("Driver submitted " + description.command.mainClass)
        val driver = createDriver(description)
        persistenceEngine.addDriver(driver)
        waitingDrivers += driver
        drivers.add(driver)
        schedule()

        // TODO: It might be good to instead have the submission client poll the master to determine
        //       the current status of the driver. For now it's simply "fire and forget".

        sender ! SubmitDriverResponse(true, Some(driver.id),
          s"Driver successfully submitted as ${driver.id}")
      }
    }

    case RequestKillDriver(driverId) => {
      if (state != RecoveryState.ALIVE) {
        val msg = s"Can only kill drivers in ALIVE state. Current state: $state."
        sender ! KillDriverResponse(driverId, success = false, msg)
      } else {
        logInfo("Asked to kill driver " + driverId)
        val driver = drivers.find(_.id == driverId)
        driver match {
          case Some(d) =>
            if (waitingDrivers.contains(d)) {
              waitingDrivers -= d
              self ! DriverStateChanged(driverId, DriverState.KILLED, None)
            } else {
              // We just notify the worker to kill the driver here. The final bookkeeping occurs
              // on the return path when the worker submits a state change back to the master
              // to notify it that the driver was successfully killed.
              d.worker.foreach { w =>
                w.actor ! KillDriver(driverId)
              }
            }
            // TODO: It would be nice for this to be a synchronous response
            val msg = s"Kill request for $driverId submitted"
            logInfo(msg)
            sender ! KillDriverResponse(driverId, success = true, msg)
          case None =>
            val msg = s"Driver $driverId has already finished or does not exist"
            logWarning(msg)
            sender ! KillDriverResponse(driverId, success = false, msg)
        }
      }
    }

    case RequestDriverStatus(driverId) => {
      (drivers ++ completedDrivers).find(_.id == driverId) match {
        case Some(driver) =>
          sender ! DriverStatusResponse(found = true, Some(driver.state),
            driver.worker.map(_.id), driver.worker.map(_.hostPort), driver.exception)
        case None =>
          sender ! DriverStatusResponse(found = false, None, None, None, None)
      }
    }
    //client发送的注册应用程序信息（在AppClient.scala的第83行）
    //master处理Application注册的请求
    case RegisterApplication(description) => {
      if (state == RecoveryState.STANDBY) {
        // ignore, don't send response
      } else {
        logInfo("Registering app " + description.name)
        val app = createApplication(description, sender) //将应用程序信息和发送方（client）的地址封装
        registerApplication(app) //注册这个application
        logInfo("Registered app " + description.name + " with ID " + app.id)
        persistenceEngine.addApplication(app) //持久化到磁盘，避免这个master宕机，用于恢复
        sender ! RegisteredApplication(app.id, masterUrl) //向client发送注册成功信息//反向，向SparkDeploySchedulerBackend的AppClient中的client actor发送RegisteredApplication
        schedule()  //进行资源调度
      }
    }
    //master接收到client发送过来的 已知Executor状态改变的消息的回复
    //master接收到worker发送过来的 已知Executor状态改变的消息的回复
    case ExecutorStateChanged(appId, execId, state, message, exitStatus) => {
      //找到executor对应的app，然后返过来通过app内部的executors缓存获取executor信息
      val execOption = idToApp.get(appId).flatMap(app => app.executors.get(execId))
      execOption match {
        case Some(exec) => {
          val appInfo = idToApp(appId)
          exec.state = state //设置状态
          if (state == ExecutorState.RUNNING) { appInfo.resetRetryCount() }
          //向driver同步发布ExecutorUpdated消息
          exec.application.driver ! ExecutorUpdated(execId, state, message, exitStatus)
          //如果executor状态是完成了
          if (ExecutorState.isFinished(state)) {
            // Remove this executor from the worker and app
            logInfo(s"Removing executor ${exec.fullId} because it is $state")
            //从 appInfo的缓存中移除executor
            appInfo.removeExecutor(exec)
            //从运行executor的worker缓存中移除executor
            exec.worker.removeExecutor(exec)

            val normalExit = exitStatus == Some(0) //如果退出状态为0，则认为是正常退出
            // Only retry certain number of times so we don't go into an infinite loop.
            //如果不是Executor不是正常退出，则进行有限次数的尝试
            if (!normalExit) {
              if (appInfo.incrementRetryCount() < ApplicationState.MAX_NUM_RETRY) {  //10
                schedule() //重新进行调度
              } else {
                //否则如果反复调度都是失败的，达到了最大的重试次数，
                // 那么就认为application是失败了，进行removeApplication
                val execs = appInfo.executors.values
                if (!execs.exists(_.state == ExecutorState.RUNNING)) {
                  logError(s"Application ${appInfo.desc.name} with ID ${appInfo.id} failed " +
                    s"${appInfo.retryCount} times; removing it")
                  removeApplication(appInfo, ApplicationState.FAILED)
                }
              }
            }
          }
        }
        case None =>
          logWarning(s"Got status update for unknown executor $appId/$execId")
      }
    } //case ExecutorStateChanged

    case DriverStateChanged(driverId, state, exception) => {
      state match {
        //如果driver的状态是错误、完成、被杀掉、失败
        //那么就移除driver
        case DriverState.ERROR | DriverState.FINISHED | DriverState.KILLED | DriverState.FAILED =>
          removeDriver(driverId, state, exception)
        //否则没有需要更新driver的状态
        case _ =>
          throw new Exception(s"Received unexpected state update for driver $driverId: $state")
      }
    }

    case Heartbeat(workerId) => {
      idToWorker.get(workerId) match {
        case Some(workerInfo) =>
          workerInfo.lastHeartbeat = System.currentTimeMillis()
        case None =>
          if (workers.map(_.id).contains(workerId)) {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " Asking it to re-register.")
            sender ! ReconnectWorker(masterUrl)
          } else {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " This worker was never registered, so ignoring the heartbeat.")
          }
      }
    }

    case MasterChangeAcknowledged(appId) => {
      idToApp.get(appId) match {
        case Some(app) =>
          logInfo("Application has been re-registered: " + appId)
          app.state = ApplicationState.WAITING
        case None =>
          logWarning("Master change ack from unknown app: " + appId)
      }

      if (canCompleteRecovery) { completeRecovery() }
    }

    case WorkerSchedulerStateResponse(workerId, executors, driverIds) => {
      idToWorker.get(workerId) match {
        case Some(worker) =>
          logInfo("Worker has been re-registered: " + workerId)
          worker.state = WorkerState.ALIVE

          val validExecutors = executors.filter(exec => idToApp.get(exec.appId).isDefined)
          for (exec <- validExecutors) {
            val app = idToApp.get(exec.appId).get
            val execInfo = app.addExecutor(worker, exec.cores, Some(exec.execId))
            worker.addExecutor(execInfo)
            execInfo.copyState(exec)
          }

          for (driverId <- driverIds) {
            drivers.find(_.id == driverId).foreach { driver =>
              driver.worker = Some(worker)
              driver.state = DriverState.RUNNING
              worker.drivers(driverId) = driver
            }
          }
        case None =>
          logWarning("Scheduler state from unknown worker: " + workerId)
      }

      if (canCompleteRecovery) { completeRecovery() }
    }

    case DisassociatedEvent(_, address, _) => {
      // The disconnected client could've been either a worker or an app; remove whichever it was
      logInfo(s"$address got disassociated, removing it.")
      addressToWorker.get(address).foreach(removeWorker)
      addressToApp.get(address).foreach(finishApplication)
      if (state == RecoveryState.RECOVERING && canCompleteRecovery) { completeRecovery() }
    }

    case RequestMasterState => {
      sender ! MasterStateResponse(
        host, port, restServerBoundPort,
        workers.toArray, apps.toArray, completedApps.toArray,
        drivers.toArray, completedDrivers.toArray, state)
    }

    case CheckForWorkerTimeOut => {  //接收到自己给自己发送的case object定时信息，用来检测超时Worker节点
      timeOutDeadWorkers()
    }

    case BoundPortsRequest => {
      sender ! BoundPortsResponse(port, webUi.boundPort, restServerBoundPort)
    }
  }

  def canCompleteRecovery =
    workers.count(_.state == WorkerState.UNKNOWN) == 0 &&
      apps.count(_.state == ApplicationState.UNKNOWN) == 0

  def beginRecovery(storedApps: Seq[ApplicationInfo], storedDrivers: Seq[DriverInfo],
      storedWorkers: Seq[WorkerInfo]) {
    for (app <- storedApps) {
      logInfo("Trying to recover app: " + app.id)
      try {
        registerApplication(app)
        app.state = ApplicationState.UNKNOWN
        app.driver ! MasterChanged(masterUrl, masterWebUiUrl)
      } catch {
        case e: Exception => logInfo("App " + app.id + " had exception on reconnect")
      }
    }

    for (driver <- storedDrivers) {
      // Here we just read in the list of drivers. Any drivers associated with now-lost workers
      // will be re-launched when we detect that the worker is missing.
      drivers += driver
    }

    for (worker <- storedWorkers) {
      logInfo("Trying to recover worker: " + worker.id)
      try {
        registerWorker(worker)
        worker.state = WorkerState.UNKNOWN
        worker.actor ! MasterChanged(masterUrl, masterWebUiUrl)
      } catch {
        case e: Exception => logInfo("Worker " + worker.id + " had exception on reconnect")
      }
    }
  }

  /**
   * 完成master的主备切换，从字面意思来说，就是完成master的恢复
   * */
  def completeRecovery() {
    // Ensure "only-once" recovery semantics using a short synchronization period.
    //确定是否状态已经修改完成
    synchronized {
      if (state != RecoveryState.RECOVERING) { return }
      state = RecoveryState.COMPLETING_RECOVERY
    }

    // Kill off any workers and apps that didn't respond to us.
    //主备切换之前，会将所有的Application和Worker的状态改为UNKNOWN，响应新master的Application和Worker状态就改为不是UNKNOWN
    //所以这里Worker要过滤出来状态是UNKNOWN的，Application要过滤出来状态是UNKNOWN的
    //然后分别调用removeWorker方法和finishApplication方法，对可能已经出故障或者已经死掉的Worker和Application进行清理
    //清理的机制：三点：1）从内存缓存结构中移除2）从相关的组件的内存缓存中移除3）从持久化存储中移除
    workers.filter(_.state == WorkerState.UNKNOWN).foreach(removeWorker)
    apps.filter(_.state == ApplicationState.UNKNOWN).foreach(finishApplication)

    // Reschedule drivers which were not claimed by any workers
    //如果driver的连接的worker是空的，进行重新发布连接
    drivers.filter(_.worker.isEmpty).foreach { d =>
      logWarning(s"Driver ${d.id} was not found after master recovery")
      if (d.desc.supervise) {
        logWarning(s"Re-launching ${d.id}")
        relaunchDriver(d)
      } else {
        removeDriver(d.id, DriverState.ERROR, None)
        logWarning(s"Did not re-launch ${d.id} because it was not supervised")
      }
    }

    state = RecoveryState.ALIVE
    schedule()
    logInfo("Recovery complete - resuming operations!")
  }

  /**
   * Can an app use the given worker? True if the worker has enough memory and we haven't already
   * launched an executor for the app on it (right now the standalone backend doesn't like having
   * two executors on the same worker).
   */
  //对于一台worker节点，对于同一个app只能启一个worker
  def canUse(app: ApplicationInfo, worker: WorkerInfo): Boolean = {
    //workder当前剩余的内存>appp在每个worker其要求的内存 且这个worker没有启动这个app的executor
    worker.memoryFree >= app.desc.memoryPerSlave && !worker.hasExecutor(app)
  }

  /**
   * Schedule the currently available resources among waiting apps. This method will be called
   * every time a new app joins or resource availability changes.
   */
  private def schedule() {
    //Master状态不是ALIVE，直接返回
    //也就是说，standby master是不会进行application等资源调度的
    if (state != RecoveryState.ALIVE) { return }

    // First schedule drivers, they take strict precedence over applications
    // Randomization helps balance drivers
    //Random.shuffle是对传入的元素进行随机打乱
    //取出了workers中的所有之前注册过的worker信息，并过滤，必须是状态ALIVE的worker
    // 对状态为ALIVE的worker，调用Random的shuffle方法进行随机打乱
    val shuffledAliveWorkers = Random.shuffle(workers.toSeq.filter(_.state == WorkerState.ALIVE))
    val numWorkersAlive = shuffledAliveWorkers.size
    var curPos = 0
    //首先调度driver，在用yarn-cluster提交的时候，才会注册driver
    //因为standalone和yarn-client模式，都会在本地直接启动driver，而不会来注册driver，就不会让master来调度driver了

    //driver的调度机制
    //遍历waitingDrivers ArrayBuffer
    for (driver <- waitingDrivers.toList) { // iterate over a copy of waitingDrivers
      // We assign workers to each waiting driver in a round-robin fashion. For each driver, we
      // start from the last worker that was assigned a driver, and continue onwards until we have
      // explored all alive workers.
      var launched = false
      var numWorkersVisited = 0
      //只要有活着的worker没有遍历到，而且当前driver还没有启动，那么就继续遍历
      while (numWorkersVisited < numWorkersAlive && !launched) {
        val worker = shuffledAliveWorkers(curPos) //拿到一个worker
        numWorkersVisited += 1 //遍历过的worker个数加1
        //如果该worker的空闲内存和核数都大于等于driver需要的内存和核数
        if (worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores) {
          //启动在该worker中启动driver
          launchDriver(worker, driver) //
          //并且等待调度driver的序列中移除该driver
          waitingDrivers -= driver
          launched = true
        }
        curPos = (curPos + 1) % numWorkersAlive //改变指针
      }
    }

    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc.
    //Application的 调度机制（核心）
    //Application的调度算法有两种，一种是spreadOutApps，另一种是非spreadOutApps
    //两种方法（尽量打散和尽量集中）
    if (spreadOutApps) { //尽量打散Application
      // Try to spread out each app among all the nodes, until it has all its cores
      //尝试将每个app发布到尽可能多的节点，直到满足核的需要
      //遍历waitingApps并且该app的剩余需要核数>0
      for (app <- waitingApps if app.coresLeft > 0) { //遍历等待app队列
        val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)//遍历worker的信息，查找ALIVE的worker
          .filter(canUse(app, _)).sortBy(_.coresFree).reverse //按能够使用（内存和对于一台worker节点，对于同一个app只能启一个worker）的每个节点的剩余核数从高到低排序
        val numUsable = usableWorkers.length   //可使用worker数量
        //创建一个空数组，存储了要分配给每个worker的cpu数量
        val assigned = new Array[Int](numUsable) // Number of cores to give on each node
        var toAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum) //需要分配和worker总核数取最小
        var pos = 0
        while (toAssign > 0) { //跳出while循环，要么该Application满足了核数需求，要么可用核数用完了
          if (usableWorkers(pos).coresFree - assigned(pos) > 0) { //如果当前机器剩余核数>需要分配的核数
            toAssign -= 1  //toAssign -1，先每个机器分配1个核
            assigned(pos) += 1 //记录节点对应位置的已分配核数+1
          }
          pos = (pos + 1) % numUsable //如果遍历一遍了，再次遍历一遍
        }
        // Now that we've decided how many cores to give on each node, let's actually give them
        //给每个worker分配完Application要求的cpu core之后
        //遍历worker
        for (pos <- 0 until numUsable) {
          if (assigned(pos) > 0) {  //如果对应机器位置启动的核数>0,则需要启动executor
            //在Application内部缓存结构中 ，添加executor
            //并且创建ExecutorDesc对象，其中封装了，给这个Executor分配多少个cpu core
            //这是spark 1.3.0版本的Executor启动的内部机制
            //在spark-submit脚本中，可以指定要多少个executor，每个executor多少个cpu，多少个内存
            //那么基于这个机制，实际上最后，executor的实际数量，以及每个executor的cpu，可能与配置是不一样的
            //因为，这里是基于总的cpu来分配的var toAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)
            //比如要求3个executor，每个要3个cpu，有个9个worker，每个有1个cpu，
            // 那么其实要分配9个core，会给每个worker分配一个core，然后给每个worker启动一个executor，
            // 最后会启动9个executor，每个executor占有1一个cpu core
            val exec = app.addExecutor(usableWorkers(pos), assigned(pos))
            launchExecutor(usableWorkers(pos), exec)
            //将Application状态设置为RUNNING
            app.state = ApplicationState.RUNNING
          }
        }
      }
    } else {  //尽量集中Application
      // Pack each app into as few nodes as possible until we've assigned all its cores
      //这里workers的顺序没有特殊排序处理
      //每个application都尽可能分配到尽量少的worker上去，比如总共有10个worker，每个有10个core
      //那么每个worker都沾满10个core
      //比如，spark-submit中，配置的是要10个executor，每个要2个core，那么总共是20个core
      //但是在这种算法下，其实总共只会启动2个executor，每个有10个core

      //首先遍历worker，并且状态是ALIVE，还有空闲cpu的worker
      for (worker <- workers if worker.coresFree > 0 && worker.state == WorkerState.ALIVE) {
        //遍历Application，并且是还有需要分配的core的application
        for (app <- waitingApps if app.coresLeft > 0) { //coresLeft是一个方法！！！
          //判断该worker可以被application使用
          if (canUse(app, worker)) {
            val coresToUse = math.min(worker.coresFree, app.coresLeft) //这个worker剩余核数和app需要核数取最小
            if (coresToUse > 0) { //当前主机能有核的分配，就分配
              val exec = app.addExecutor(worker, coresToUse)
              launchExecutor(worker, exec)   //这个worker启动一个executor
              //将该application的状态修改为RUNNING
              app.state = ApplicationState.RUNNING
            }
          }
        }
      }
    } //else
  }

  def launchExecutor(worker: WorkerInfo, exec: ExecutorDesc) {
    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
    worker.addExecutor(exec) //记录该worker使用的资源，进行计算资源的改变
    worker.actor ! LaunchExecutor(masterUrl,   //master向worker发送启动Executor消息，消息是一个case class
      exec.application.id, exec.id, exec.application.desc, exec.cores, exec.memory)
    //maser向client actor发送消息，告诉他Executor已经启动
    exec.application.driver ! ExecutorAdded(
      exec.id, worker.id, worker.hostPort, exec.cores, exec.memory)
  }

  def registerWorker(worker: WorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (w/ different ID's),
    // remove them.
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
    }.foreach { w =>
      workers -= w
    }

    val workerAddress = worker.actor.path.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        removeWorker(oldWorker)
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        return false
      }
    }

    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker
    true
  }

  /**
   * //删除worker节点
   * @param worker
   */
  def removeWorker(worker: WorkerInfo) {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    worker.setState(WorkerState.DEAD) //将Worker的状态设置为DEAD
    idToWorker -= worker.id
    addressToWorker -= worker.actor.path.address
    for (exec <- worker.executors.values) { //遍历这些worker上的Executor
      logInfo("Telling app of lost executor: " + exec.id)
      exec.application.driver ! ExecutorUpdated(
        exec.id, ExecutorState.LOST, Some("worker lost"), None)
      exec.application.removeExecutor(exec) //删除Executor
    }
    for (driver <- worker.drivers.values) {  //对于worker连接的driver
      if (driver.desc.supervise) { //如果配置了driver.desc.supervise这个属性为true，会尝试重连
        logInfo(s"Re-launching ${driver.id}")
        relaunchDriver(driver)
      } else { //如果没有配置这个属性，则将driver也删除掉
        logInfo(s"Not re-launching ${driver.id} because it was not supervised")
        removeDriver(driver.id, DriverState.ERROR, None)
      }
    }
    persistenceEngine.removeWorker(worker)  //将该worker从持久化引擎中删除掉
  }

  def relaunchDriver(driver: DriverInfo) {
    driver.worker = None
    driver.state = DriverState.RELAUNCHING
    waitingDrivers += driver
    schedule()
  }

  def createApplication(desc: ApplicationDescription, driver: ActorRef): ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new ApplicationInfo(now, newApplicationId(date), desc, date, driver, defaultCores)
  }

  /**
   * //master注册Application信息
   * @param app
   */
  def registerApplication(app: ApplicationInfo): Unit = {
    val appAddress = app.driver.path.address
    if (addressToApp.contains(appAddress)) {
      logInfo("Attempted to re-register application at same address: " + appAddress)
      return
    }

    applicationMetricsSystem.registerSource(app.appSource)
    apps += app  //加入application的列表
    idToApp(app.id) = app
    actorToApp(app.driver) = app //加入application与driver:ActorRef的对应关系
    addressToApp(appAddress) = app
    waitingApps += app  //加入等待队列
  }

  def finishApplication(app: ApplicationInfo) {
    removeApplication(app, ApplicationState.FINISHED)
  }

  def removeApplication(app: ApplicationInfo, state: ApplicationState.Value) {
    if (apps.contains(app)) {
      logInfo("Removing app " + app.id)
      apps -= app
      idToApp -= app.id
      actorToApp -= app.driver
      addressToApp -= app.driver.path.address
      if (completedApps.size >= RETAINED_APPLICATIONS) {
        val toRemove = math.max(RETAINED_APPLICATIONS / 10, 1)
        completedApps.take(toRemove).foreach( a => {
          appIdToUI.remove(a.id).foreach { ui => webUi.detachSparkUI(ui) }
          applicationMetricsSystem.removeSource(a.appSource)
        })
        completedApps.trimStart(toRemove)
      }
      completedApps += app // Remember it in our history
      waitingApps -= app

      // If application events are logged, use them to rebuild the UI
      rebuildSparkUI(app)

      for (exec <- app.executors.values) {
        exec.worker.removeExecutor(exec)
        exec.worker.actor ! KillExecutor(masterUrl, exec.application.id, exec.id)
        exec.state = ExecutorState.KILLED
      }
      app.markFinished(state)
      if (state != ApplicationState.FINISHED) {
        app.driver ! ApplicationRemoved(state.toString)
      }
      persistenceEngine.removeApplication(app) //从持久化引擎中删除app
      schedule()

      // Tell all workers that the application has finished, so they can clean up any app state.
      workers.foreach { w =>
        w.actor ! ApplicationFinished(app.id)
      }
    }
  }

  /**
   * Rebuild a new SparkUI from the given application's event logs.
   * Return whether this is successful.
   */
  def rebuildSparkUI(app: ApplicationInfo): Boolean = {
    val appName = app.desc.name
    val notFoundBasePath = HistoryServer.UI_PATH_PREFIX + "/not-found"
    try {
      val eventLogFile = app.desc.eventLogDir
        .map { dir => EventLoggingListener.getLogPath(dir, app.id, app.desc.eventLogCodec) }
        .getOrElse {
          // Event logging is not enabled for this application
          app.desc.appUiUrl = notFoundBasePath
          return false
        }

      val fs = Utils.getHadoopFileSystem(eventLogFile, hadoopConf)

      if (fs.exists(new Path(eventLogFile + EventLoggingListener.IN_PROGRESS))) {
        // Event logging is enabled for this application, but the application is still in progress
        val title = s"Application history not found (${app.id})"
        var msg = s"Application $appName is still in progress."
        logWarning(msg)
        msg = URLEncoder.encode(msg, "UTF-8")
        app.desc.appUiUrl = notFoundBasePath + s"?msg=$msg&title=$title"
        return false
      }

      val logInput = EventLoggingListener.openEventLog(new Path(eventLogFile), fs)
      val replayBus = new ReplayListenerBus()
      val ui = SparkUI.createHistoryUI(new SparkConf, replayBus, new SecurityManager(conf),
        appName + " (completed)", HistoryServer.UI_PATH_PREFIX + s"/${app.id}")
      try {
        replayBus.replay(logInput, eventLogFile)
      } finally {
        logInput.close()
      }
      appIdToUI(app.id) = ui
      webUi.attachSparkUI(ui)
      // Application UI is successfully rebuilt, so link the Master UI to it
      app.desc.appUiUrl = ui.basePath
      true
    } catch {
      case fnf: FileNotFoundException =>
        // Event logging is enabled for this application, but no event logs are found
        val title = s"Application history not found (${app.id})"
        var msg = s"No event logs found for application $appName in ${app.desc.eventLogDir}."
        logWarning(msg)
        msg += " Did you specify the correct logging directory?"
        msg = URLEncoder.encode(msg, "UTF-8")
        app.desc.appUiUrl = notFoundBasePath + s"?msg=$msg&title=$title"
        false
      case e: Exception =>
        // Relay exception message to application UI page
        val title = s"Application history load error (${app.id})"
        val exception = URLEncoder.encode(Utils.exceptionString(e), "UTF-8")
        var msg = s"Exception in replaying log for application $appName!"
        logError(msg, e)
        msg = URLEncoder.encode(msg, "UTF-8")
        app.desc.appUiUrl = notFoundBasePath + s"?msg=$msg&exception=$exception&title=$title"
        false
    }
  }

  /** Generate a new app ID given a app's submission date */
  def newApplicationId(submitDate: Date): String = {
    val appId = "app-%s-%04d".format(createDateFormat.format(submitDate), nextAppNumber)
    nextAppNumber += 1
    appId
  }

  /** Check for, and remove, any timed-out workers */
  //用来检测超时Worker节点的方法
  def timeOutDeadWorkers() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT).toArray
    for (worker <- toRemove) {
      if (worker.state != WorkerState.DEAD) {
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          worker.id, WORKER_TIMEOUT/1000))
        removeWorker(worker)
      } else {
        if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT)) {
          workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
        }
      }
    }
  }

  def newDriverId(submitDate: Date): String = {
    val appId = "driver-%s-%04d".format(createDateFormat.format(submitDate), nextDriverNumber)
    nextDriverNumber += 1
    appId
  }

  def createDriver(desc: DriverDescription): DriverInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new DriverInfo(now, newDriverId(date), desc, date)
  }

  /**
   * 在某个worker中启动driver
   * @param worker
   * @param driver
   */
  def launchDriver(worker: WorkerInfo, driver: DriverInfo) {
    logInfo("Launching driver " + driver.id + " on worker " + worker.id)
    //将driver加入worker内存的缓存结构
    worker.addDriver(driver) //将该worker的使用内存和核数都分别加上该driver使用的内存和核数
    //将worker也加入到driver的内存缓存结构中
    driver.worker = Some(worker)
    //然后调用worker的actor，给他发送LaunchDriver消息，让worker来启动Driver
    worker.actor ! LaunchDriver(driver.id, driver.desc)
    //将driver的状态设置为RUNNING
    driver.state = DriverState.RUNNING
  }

  def removeDriver(driverId: String, finalState: DriverState, exception: Option[Exception]) {
    //用 scala HashSet的find()高阶函数，找到dirverId对应的driver
    drivers.find(d => d.id == driverId) match {
        //如果找到了，Some，样例类（Option）
      case Some(driver) =>
        logInfo(s"Removing driver: $driverId")
       //将driver从内存缓存中清除
        drivers -= driver
        if (completedDrivers.size >= RETAINED_DRIVERS) {
          val toRemove = math.max(RETAINED_DRIVERS / 10, 1)
          completedDrivers.trimStart(toRemove)
        }
        //向completedDrivers中加入driver
        completedDrivers += driver
        //使用持久化引擎去除driver的持久化信息
        persistenceEngine.removeDriver(driver)
        driver.state = finalState
        driver.exception = exception
        //将driver申请的worker，移除driver
        driver.worker.foreach(w => w.removeDriver(driver))
        //同样，调用schedule方法
        schedule()
      case None =>
        logWarning(s"Asked to remove unknown driver: $driverId")
    }
  }
}

private[spark] object Master extends Logging {
  val systemName = "sparkMaster"  //定义ActorSystem名字
  private val actorName = "Master" //定义Actor名字

  def main(argStrings: Array[String]) {
    SignalLogger.register(log)
    val conf = new SparkConf
    val args = new MasterArguments(argStrings, conf)
    //创建Actor和ActorSystem
    val (actorSystem, _, _, _) = startSystemAndActor(args.host, args.port, args.webUiPort, conf)
    actorSystem.awaitTermination()    //启动
  }

  /**
   * Returns an `akka.tcp://...` URL for the Master actor given a sparkUrl `spark://host:port`.
   *
   * @throws SparkException if the url is invalid
   */
  def toAkkaUrl(sparkUrl: String, protocol: String): String = {
    val (host, port) = Utils.extractHostPortFromSparkUrl(sparkUrl)
    AkkaUtils.address(protocol, systemName, host, port, actorName)
  }

  /**
   * Returns an akka `Address` for the Master actor given a sparkUrl `spark://host:port`.
   *
   * @throws SparkException if the url is invalid
   */
  def toAkkaAddress(sparkUrl: String, protocol: String): Address = {
    val (host, port) = Utils.extractHostPortFromSparkUrl(sparkUrl)
    Address(protocol, systemName, host, port)
  }

  /**
   * Start the Master and return a four tuple of:
   *   (1) The Master actor system
   *   (2) The bound port
   *   (3) The web UI bound port
   *   (4) The REST server bound port, if any
   */
  def startSystemAndActor(
      host: String,
      port: Int,
      webUiPort: Int,
      conf: SparkConf): (ActorSystem, Int, Int, Option[Int]) = {
    val securityMgr = new SecurityManager(conf)
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port, conf = conf,
      securityManager = securityMgr)   ///创建ActorSystem
    //创建master的Actor
    val actor = actorSystem.actorOf(
      Props(classOf[Master], host, boundPort, webUiPort, securityMgr, conf), actorName)
    val timeout = AkkaUtils.askTimeout(conf)
    val portsRequest = actor.ask(BoundPortsRequest)(timeout)
    val portsResponse = Await.result(portsRequest, timeout).asInstanceOf[BoundPortsResponse]
    (actorSystem, boundPort, portsResponse.webUIPort, portsResponse.restPort)
  }
}
