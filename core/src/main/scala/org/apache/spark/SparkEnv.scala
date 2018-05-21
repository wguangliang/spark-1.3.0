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

package org.apache.spark

import java.io.File
import java.net.Socket

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Properties

import akka.actor._
import com.google.common.collect.MapMaker

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.PythonWorkerFactory
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.network.nio.NioBlockTransferService
import org.apache.spark.scheduler.{OutputCommitCoordinator, LiveListenerBus}
import org.apache.spark.scheduler.OutputCommitCoordinator.OutputCommitCoordinatorActor
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleMemoryManager, ShuffleManager}
import org.apache.spark.storage._
import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * :: DeveloperApi ::
 * Holds all the runtime environment objects for a running Spark instance (either master or worker),
 * including the serializer, Akka actor system, block manager, map output tracker, etc. Currently
 * Spark code finds the SparkEnv through a global variable, so all the threads can access the same
 * SparkEnv. It can be accessed by SparkEnv.get (e.g. after creating a SparkContext).
 *
 * NOTE: This is not intended for external use. This is exposed for Shark and may be made private
 *       in a future release.
 *
 *    SparkEnv的构造步骤如下：
  *    1）创建安全管理器SecurityManager
  *    2）创建基于Akka的分布式消息系统ActorSystem
  *    3）创建Map任务输出跟踪器mapOutputTracker
  *    4）实例化ShuffleManager
  *    5）创建ShuffleMemoryManager
  *    6）创建块传输服务BlockTransferService
  *    7）创建BlockManagerMaster
  *    8）创建块管理器 BlockManager
  *    9）创建广播管理器 CacheManger
  *    10）创建缓存管理器 CacheManager
  *    11）创建HTTP文件服务器HttpFileServer
  *    12）创建测量系统MetricsSystem
  *    13）创建SparkEnv
  *
  *
  *
  *
 */
@DeveloperApi
class SparkEnv (
    val executorId: String,
    val actorSystem: ActorSystem,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val cacheManager: CacheManager,
    val mapOutputTracker: MapOutputTracker,
    val shuffleManager: ShuffleManager,
    val broadcastManager: BroadcastManager,
    val blockTransferService: BlockTransferService,
    val blockManager: BlockManager,
    val securityManager: SecurityManager,
    val httpFileServer: HttpFileServer,
    val sparkFilesDir: String,
    val metricsSystem: MetricsSystem,
    val shuffleMemoryManager: ShuffleMemoryManager,
    val outputCommitCoordinator: OutputCommitCoordinator,
    val conf: SparkConf) extends Logging {

  private[spark] var isStopped = false
  private val pythonWorkers = mutable.HashMap[(String, Map[String, String]), PythonWorkerFactory]()

  // A general, soft-reference map for metadata needed during HadoopRDD split computation
  // (e.g., HadoopFileRDD uses this to cache JobConfs and InputFormats).
  private[spark] val hadoopJobMetadata = new MapMaker().softValues().makeMap[String, Any]()

  private[spark] def stop() {
    isStopped = true
    pythonWorkers.foreach { case(key, worker) => worker.stop() }
    Option(httpFileServer).foreach(_.stop())
    mapOutputTracker.stop()
    shuffleManager.stop()
    broadcastManager.stop()
    blockManager.stop()
    blockManager.master.stop()
    metricsSystem.stop()
    outputCommitCoordinator.stop()
    actorSystem.shutdown()
    // Unfortunately Akka's awaitTermination doesn't actually wait for the Netty server to shut
    // down, but let's call it anyway in case it gets fixed in a later release
    // UPDATE: In Akka 2.1.x, this hangs if there are remote actors, so we can't call it.
    // actorSystem.awaitTermination()

    // Note that blockTransferService is stopped by BlockManager since it is started by it.
  }

  private[spark]
  def createPythonWorker(pythonExec: String, envVars: Map[String, String]): java.net.Socket = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars)).create()
    }
  }

  private[spark]
  def destroyPythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.stopWorker(worker))
    }
  }

  private[spark]
  def releasePythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.releaseWorker(worker))
    }
  }
}

object SparkEnv extends Logging {
  @volatile private var env: SparkEnv = _

  private[spark] val driverActorSystemName = "sparkDriver"
  private[spark] val executorActorSystemName = "sparkExecutor"

  def set(e: SparkEnv) {
    env = e
  }

  /**
   * Returns the SparkEnv.
   */
  def get: SparkEnv = {
    env
  }

  /**
   * Returns the ThreadLocal SparkEnv.
   */
  @deprecated("Use SparkEnv.get instead", "1.2")
  def getThreadLocal: SparkEnv = {
    env
  }

  /**
   * Create a SparkEnv for the driver.
   */
  private[spark] def createDriverEnv(
      conf: SparkConf,
      isLocal: Boolean,
      listenerBus: LiveListenerBus,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
    assert(conf.contains("spark.driver.host"), "spark.driver.host is not set on the driver!")
    assert(conf.contains("spark.driver.port"), "spark.driver.port is not set on the driver!")
    val hostname = conf.get("spark.driver.host")
    val port = conf.get("spark.driver.port").toInt
    create(
      conf,
      SparkContext.DRIVER_IDENTIFIER,
      hostname,
      port,
      isDriver = true,
      isLocal = isLocal,
      listenerBus = listenerBus,
      mockOutputCommitCoordinator = mockOutputCommitCoordinator
    )
  }

  /**
   * Create a SparkEnv for an executor.
   * In coarse-grained mode, the executor provides an actor system that is already instantiated.
   */
  private[spark] def createExecutorEnv(
      conf: SparkConf,
      executorId: String,
      hostname: String,
      port: Int,
      numCores: Int,
      isLocal: Boolean): SparkEnv = {
    val env = create(
      conf,
      executorId,
      hostname,
      port,
      isDriver = false,
      isLocal = isLocal,
      numUsableCores = numCores
    )
    SparkEnv.set(env)
    env
  }

  /**
   * Helper method to create a SparkEnv for a driver or an executor.
   */
  private def create(
      conf: SparkConf,
      executorId: String,
      hostname: String,
      port: Int,
      isDriver: Boolean,
      isLocal: Boolean,
      listenerBus: LiveListenerBus = null,
      numUsableCores: Int = 0,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {

    // Listener bus is only used on the driver
    if (isDriver) {
      assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
    }


    // ToDo 1）创建安全管理器SecurityManager

    val securityManager = new SecurityManager(conf)

    // Create the ActorSystem for Akka and get the port it binds to.
    // ToDo 2） 创建ActorSystem
    val (actorSystem, boundPort) = {
      val actorSystemName = if (isDriver) driverActorSystemName else executorActorSystemName
      AkkaUtils.createActorSystem(actorSystemName, hostname, port, conf, securityManager)
    }

    // Figure out which port Akka actually bound to in case the original port is 0 or occupied.
    if (isDriver) {
      conf.set("spark.driver.port", boundPort.toString)
    } else {
      conf.set("spark.executor.port", boundPort.toString)
    }

    // Create an instance of the class with the given name, possibly initializing it with our conf
    def instantiateClass[T](className: String): T = {
      val cls = Class.forName(className, true, Utils.getContextOrSparkClassLoader)
      // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
      // SparkConf, then one taking no arguments
      try {
        cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
          .newInstance(conf, new java.lang.Boolean(isDriver))
          .asInstanceOf[T]
      } catch {
        case _: NoSuchMethodException =>
          try {
            cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
          } catch {
            case _: NoSuchMethodException =>
              cls.getConstructor().newInstance().asInstanceOf[T]
          }
      }
    }

    // Create an instance of the class named by the given SparkConf property, or defaultClassName
    // if the property is not set, possibly initializing it with our conf
    def instantiateClassFromConf[T](propertyName: String, defaultClassName: String): T = {
      instantiateClass[T](conf.get(propertyName, defaultClassName))
    }

    /**
      *  spark env的一些默认配置：
      *  spark的序列化方式：org.apache.spark.serializer.JavaSerializer
      */
    val serializer = instantiateClassFromConf[Serializer](
      "spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    logDebug(s"Using serializer: ${serializer.getClass}")

    val closureSerializer = instantiateClassFromConf[Serializer](
      "spark.closure.serializer", "org.apache.spark.serializer.JavaSerializer")

    def registerOrLookup(name: String, newActor: => Actor): ActorRef = {
      // 如果是driver，则注册
      if (isDriver) {
        logInfo("Registering " + name)
        actorSystem.actorOf(Props(newActor), name = name)
      } else {
        // 如果是Executor，则通过调用AkkaUtils.makeDriverDef找到mapOutputTrackerMasterActor
        AkkaUtils.makeDriverRef(name, conf, actorSystem)
      }
    }

    // ToDo 3）map任务输出跟踪器mapOutputTracker
    // mapOutputTracker用于跟踪map阶段任务的输出状态，此状态便于reduce阶段任务获取地址及中间输出结果。
    // 每个map任务或reduce任务都会有其唯一标识，分别为mapId和reduceId。每个reduce任务的输入可能是多个map任务的输出，
    //    reduce会到各个map任务的所有节点上拉取Block。这个过程叫做shuffle。每批shuffle过程都有唯一的标识shuffleId
    val mapOutputTracker =  if (isDriver) {
      new MapOutputTrackerMaster(conf) // 是driver，则创建 MapOutputTrackerMaster，然后创建MapOutputTrackerMasterActor，并且注册到ActorSystem
    } else {
      new MapOutputTrackerWorker(conf) // 不是driver，是Executor，则创建 MapOutputTrackerWorker， 并从ActorSystem中找到MapOutputTrackerMasterActor
    }

    // Have to assign trackerActor after initialization as MapOutputTrackerActor
    // requires the MapOutputTracker itself
    // MapOutputTrackerMasterActor和MapOutputTrackerWorkerActor进行关联
    // map任务的状态正式由Executor向持有的MapOutputTrackerMasterActor发送消息，将map任务状态同步到mapOutputTracker的mapStatuses和cachedSerializedStatuses的
    mapOutputTracker.trackerActor = registerOrLookup(
      "MapOutputTracker",
      new MapOutputTrackerMasterActor(mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))

    // Let the user specify short names for shuffle managers
    /**
      *  默认shuffle方式
      *  Todo 4）实例化ShuffleManager
      *  ShuffleManager 负责管理本地及远程的block数据的shuffle操作
      */
    val shortShuffleMgrNames = Map(
      "hash" -> "org.apache.spark.shuffle.hash.HashShuffleManager",
      "sort" -> "org.apache.spark.shuffle.sort.SortShuffleManager")
    val shuffleMgrName = conf.get("spark.shuffle.manager", "sort") // 可以修改为shuffle来显示控制使用使用HashShuffleManager
    val shuffleMgrClass = shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase, shuffleMgrName)
    // 默认为通过反射方式生成的SortShuffleManager实例
    // SortShuffleManager间接操作BlockManager中的DiskBlockManager将map结果写入本地，并根据shuffleId、mapId写入索引文件，
    //  也能通过MapOutputTrackerMaster中维护的mapStatuses从本地或者其他远程节点读取文件
    val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)

    // ToDo 5）创建shufflememorymanger，负责管理shuffle线程占用内存的分配和释放，并通过threadMemory:mutable.HashMap[Long, Long]缓存每个线程的内存字节数
    val shuffleMemoryManager = new ShuffleMemoryManager(conf)

    // ToDo 6）块传输服务BlockTransferService
    //  默认为NettyBlockTransferService(可以配置属性spark.shuffle.blockTransferService使用NioBlockTransferService)
    val blockTransferService =
      conf.get("spark.shuffle.blockTransferService", "netty").toLowerCase match {
        case "netty" =>
          new NettyBlockTransferService(conf, securityManager, numUsableCores)
        case "nio" =>
          new NioBlockTransferService(conf, securityManager)
      }

    // ToDo 7）创建BlockManagerMaster负责对Block的管理和协调。具体操作依赖于BlockManagerMasterActor。
    //    Driver和Executor处理BlockManagerMaster的方式不同：
    //      如果当前应用程序是driver，则创建BlockManagerMasterActor，并且注册到ActorSystem中
    //      如果当前应用程序是Executor，则从ActorSystem中找到BlockManagerMasterActor
    val blockManagerMaster = new BlockManagerMaster(registerOrLookup(
      "BlockManagerMaster",
      new BlockManagerMasterActor(isLocal, conf, listenerBus)), conf, isDriver)

    // ToDo 8）创建块管理器 BlockManager
    //  负责对Block的管理。只有在BlockManager的初始化方法initialize被调用后，它才是有效的
    // NB: blockManager is not valid until initialize() is called later.
    val blockManager = new BlockManager(executorId, actorSystem, blockManagerMaster,
      serializer, conf, mapOutputTracker, shuffleManager, blockTransferService, securityManager,
      numUsableCores)

    // ToDo 9）创建广播管理器
    //  用于将配置信息和序列化后的RDD、Job以及ShuffleDependence等信息在本地存储
    val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)

    // ToDo 10）创建缓存管理器 CacheManager。
    //   用于缓存RDD某个分区计算后的中间结果
    val cacheManager = new CacheManager(blockManager)

    // ToDo 11）HTTP文件服务器HttpFileServer
    //   主要提供对jar及其他文件的http访问。这些jar包括用户上传的jar包。端口由属性spark.fileserver.port配置，默认为0，表示随机生成端口号
    val httpFileServer =
     // 只有在driver端创建
      if (isDriver) {
        val fileServerPort = conf.getInt("spark.fileserver.port", 0)  // 默认为0，表示随机生成端口号
        val server = new HttpFileServer(conf, securityManager, fileServerPort)
        server.initialize()
        conf.set("spark.fileserver.uri",  server.serverUri)
        server
      } else {
        null
      }

    // ToDo 12）创建Spark的测量系统
    //  衡量系统的各种指标的度量
    val metricsSystem = if (isDriver) {
      // Don't start metrics system right now for Driver.
      // We need to wait for the task scheduler to give us an app ID.
      // Then we can start the metrics system.
      MetricsSystem.createMetricsSystem("driver", conf, securityManager)
    } else {
      // We need to set the executor ID before the MetricsSystem is created because sources and
      // sinks specified in the metrics configuration file will want to incorporate this executor's
      // ID into the metrics they report.
      conf.set("spark.executor.id", executorId)
      val ms = MetricsSystem.createMetricsSystem("executor", conf, securityManager)
      ms.start()
      ms
    }

    // Set the sparkFiles directory, used when downloading dependencies.  In local mode,
    // this is a temporary directory; in distributed mode, this is the executor's current working
    // directory.
    val sparkFilesDir: String = if (isDriver) {
      Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
    } else {
      "."
    }

    // Warn about deprecated spark.cache.class property
    if (conf.contains("spark.cache.class")) {
      logWarning("The spark.cache.class property is no longer being used! Specify storage " +
        "levels using the RDD.persist() method instead.")
    }

    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
      new OutputCommitCoordinator(conf)
    }
    val outputCommitCoordinatorActor = registerOrLookup("OutputCommitCoordinator",
      new OutputCommitCoordinatorActor(outputCommitCoordinator))
    outputCommitCoordinator.coordinatorActor = Some(outputCommitCoordinatorActor)

    // ToDo 创建SparkEnv
    new SparkEnv(
      executorId,
      actorSystem,
      serializer,
      closureSerializer,
      cacheManager,
      mapOutputTracker,
      shuffleManager,
      broadcastManager,
      blockTransferService,
      blockManager,
      securityManager,
      httpFileServer,
      sparkFilesDir,
      metricsSystem,
      shuffleMemoryManager,
      outputCommitCoordinator,
      conf)
  }

  /**
   * Return a map representation of jvm information, Spark properties, system properties, and
   * class paths. Map keys define the category, and map values represent the corresponding
   * attributes as a sequence of KV pairs. This is used mainly for SparkListenerEnvironmentUpdate.
   */
  private[spark]
  def environmentDetails(
      conf: SparkConf,
      schedulingMode: String,
      addedJars: Seq[String],
      addedFiles: Seq[String]): Map[String, Seq[(String, String)]] = {

    import Properties._
    val jvmInformation = Seq(
      ("Java Version", s"$javaVersion ($javaVendor)"),
      ("Java Home", javaHome),
      ("Scala Version", versionString)
    ).sorted

    // Spark properties
    // This includes the scheduling mode whether or not it is configured (used by SparkUI)
    val schedulerMode =
      if (!conf.contains("spark.scheduler.mode")) {
        Seq(("spark.scheduler.mode", schedulingMode))
      } else {
        Seq[(String, String)]()
      }
    val sparkProperties = (conf.getAll ++ schedulerMode).sorted

    // System properties that are not java classpaths
    val systemProperties = Utils.getSystemProperties.toSeq
    val otherProperties = systemProperties.filter { case (k, _) =>
      k != "java.class.path" && !k.startsWith("spark.")
    }.sorted

    // Class paths including all added jars and files
    val classPathEntries = javaClassPath
      .split(File.pathSeparator)
      .filterNot(_.isEmpty)
      .map((_, "System Classpath"))
    val addedJarsAndFiles = (addedJars ++ addedFiles).map((_, "Added By User"))
    val classPaths = (addedJarsAndFiles ++ classPathEntries).sorted

    Map[String, Seq[(String, String)]](
      "JVM Information" -> jvmInformation,
      "Spark Properties" -> sparkProperties,
      "System Properties" -> otherProperties,
      "Classpath Entries" -> classPaths)
  }
}
