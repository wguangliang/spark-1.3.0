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

package org.apache.spark.ui

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkContext}
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageStatusListener
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.env.{EnvironmentListener, EnvironmentTab}
import org.apache.spark.ui.exec.{ExecutorsListener, ExecutorsTab}
import org.apache.spark.ui.jobs.{JobsTab, JobProgressListener, StagesTab}
import org.apache.spark.ui.storage.{StorageListener, StorageTab}

/**
 * Top level user interface for a Spark application.
 */
private[spark] class SparkUI private (
    val sc: Option[SparkContext],
    val conf: SparkConf,
    val securityManager: SecurityManager,
    val environmentListener: EnvironmentListener,
    val storageStatusListener: StorageStatusListener,
    val executorsListener: ExecutorsListener,
    val jobProgressListener: JobProgressListener,
    val storageListener: StorageListener,
    var appName: String,
    val basePath: String)
  extends WebUI(securityManager, SparkUI.getUIPort(conf), conf, basePath, "SparkUI")
  with Logging {

  val killEnabled = sc.map(_.conf.getBoolean("spark.ui.killEnabled", true)).getOrElse(false)

  /** Initialize all components of the server. */
  // 服务器初始化
  //    JobsTabl创建之后，将被attachTab方法加入SpariUI的ArrayBuffer[WebUITab]中，
  //    并且通过attachPage方法，给每一个page生成org.eclipse.jetty.servlet.ServletContextHandler,
  //    最后调用attachHandler方法将ServletContextHandler绑定到SparkUI，
  //    即加入到handlers:ArrayBuffer[ServletContextHandler]和样例类ServerInfo的rootHandler（ContextHandlerCollection）中。
  //   SparkUI继承自WebUI
  def initialize() {
    attachTab(new JobsTab(this))
    val stagesTab = new StagesTab(this)
    attachTab(stagesTab)
    attachTab(new StorageTab(this))
    attachTab(new EnvironmentTab(this))
    attachTab(new ExecutorsTab(this))
    attachHandler(createStaticHandler(SparkUI.STATIC_RESOURCE_DIR, "/static"))
    attachHandler(createRedirectHandler("/", "/jobs", basePath = basePath))
    attachHandler(
      createRedirectHandler("/stages/stage/kill", "/stages", stagesTab.handleKillRequest))
  }
  initialize()

  def getAppName = appName

  /** Set the app name for this UI. */
  def setAppName(name: String) {
    appName = name
  }

  /** Stop the server behind this web interface. Only valid after bind(). */
  override def stop() {
    super.stop()
    logInfo("Stopped Spark web UI at %s".format(appUIAddress))
  }

  /**
   * Return the application UI host:port. This does not include the scheme (http://).
   */
  private[spark] def appUIHostPort = publicHostName + ":" + boundPort

  private[spark] def appUIAddress = s"http://$appUIHostPort"
}

private[spark] abstract class SparkUITab(parent: SparkUI, prefix: String)
  extends WebUITab(parent, prefix) {

  def appName: String = parent.getAppName

}

private[spark] object SparkUI {
  val DEFAULT_PORT = 4040
  val STATIC_RESOURCE_DIR = "org/apache/spark/ui/static"

  def getUIPort(conf: SparkConf): Int = {
    conf.getInt("spark.ui.port", SparkUI.DEFAULT_PORT)
  }

  def createLiveUI(
      sc: SparkContext,
      conf: SparkConf,
      listenerBus: SparkListenerBus,
      jobProgressListener: JobProgressListener,
      securityManager: SecurityManager,
      appName: String): SparkUI =  {
    // 创建SparkUI
    create(Some(sc), conf, listenerBus, securityManager, appName,
      jobProgressListener = Some(jobProgressListener))
  }

  def createHistoryUI(
      conf: SparkConf,
      listenerBus: SparkListenerBus,
      securityManager: SecurityManager,
      appName: String,
      basePath: String): SparkUI = {
    create(None, conf, listenerBus, securityManager, appName, basePath)
  }

  /**
   * Create a new Spark UI.
   *
   * @param sc optional SparkContext; this can be None when reconstituting a UI from event logs.
   * @param jobProgressListener if supplied, this JobProgressListener will be used; otherwise, the
   *                            web UI will create and register its own JobProgressListener.
   */
  private def create(
      sc: Option[SparkContext],
      conf: SparkConf,
      listenerBus: SparkListenerBus,
      securityManager: SecurityManager,
      appName: String,
      basePath: String = "",
      jobProgressListener: Option[JobProgressListener] = None): SparkUI = {

    val _jobProgressListener: JobProgressListener = jobProgressListener.getOrElse {
      val listener = new JobProgressListener(conf)
      listenerBus.addListener(listener)
      listener
    }

    // 一共包括4个监听器SparkListener的实现：
    //  对JVM参数，spark属性，java系统属性，classpath等进行监控的EnvironmentListener
    val environmentListener = new EnvironmentListener
    //  维护Executor的存储状态的StorageStatusListener
    val storageStatusListener = new StorageStatusListener
    //  用于维护Executor的存储状态
    val executorsListener = new ExecutorsListener(storageStatusListener)
    // 用于准备将Executor相关存储信息展示在BlockManagerUI的StorageListener
    val storageListener = new StorageListener(storageStatusListener)

    listenerBus.addListener(environmentListener)
    listenerBus.addListener(storageStatusListener)
    listenerBus.addListener(executorsListener)
    listenerBus.addListener(storageListener)

    new SparkUI(sc, conf, securityManager, environmentListener, storageStatusListener,
      executorsListener, _jobProgressListener, storageListener, appName, basePath)
  }
}
