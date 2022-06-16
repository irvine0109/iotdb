/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.it.env;

import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.itbase.runtime.ClusterTestConnection;
import org.apache.iotdb.itbase.runtime.NodeConnection;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.Constant;
import org.apache.iotdb.jdbc.IoTDBConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import static org.apache.iotdb.jdbc.Config.VERSION;
import static org.junit.Assert.fail;

public abstract class AbstractEnv implements BaseEnv {
  private static final Logger logger = LoggerFactory.getLogger(AbstractEnv.class);
  protected List<ConfigNodeWrapper> configNodeWrapperList;
  protected List<DataNodeWrapper> dataNodeWrapperList;
  private final Random rand = new Random();
  private String nextTestCase = null;

  protected void initEnvironment(int configNodesNum, int dataNodesNum) throws InterruptedException {
    this.configNodeWrapperList = new ArrayList<>();
    this.dataNodeWrapperList = new ArrayList<>();

    String testName = getTestClassName();
    if (nextTestCase != null) {
      testName = testName + "_" + nextTestCase;
    }

    ConfigNodeWrapper seedConfigNodeWrapper = new ConfigNodeWrapper(true, "", testName);
    seedConfigNodeWrapper.createDir();
    seedConfigNodeWrapper.changeConfig(ConfigFactory.getConfig().getConfignodeProperties());
    seedConfigNodeWrapper.start();
    String targetConfigNode = seedConfigNodeWrapper.getIpAndPortString();
    this.configNodeWrapperList.add(seedConfigNodeWrapper);
    logger.info(
        "In test " + testName + " SeedConfigNode " + seedConfigNodeWrapper.getId() + " started.");

    for (int i = 1; i < configNodesNum; i++) {
      ConfigNodeWrapper configNodeWrapper =
          new ConfigNodeWrapper(false, targetConfigNode, testName);
      configNodeWrapper.createDir();
      configNodeWrapper.changeConfig(ConfigFactory.getConfig().getConfignodeProperties());
      configNodeWrapper.start();
      this.configNodeWrapperList.add(configNodeWrapper);
      logger.info("In test " + testName + " ConfigNode " + configNodeWrapper.getId() + " started.");
    }

    for (int i = 0; i < dataNodesNum; i++) {
      DataNodeWrapper dataNodeWrapper = new DataNodeWrapper(targetConfigNode, testName);
      dataNodeWrapper.createDir();
      dataNodeWrapper.changeConfig(ConfigFactory.getConfig().getEngineProperties());
      dataNodeWrapper.start();
      this.dataNodeWrapperList.add(dataNodeWrapper);
      logger.info("In test " + testName + " DataNode " + dataNodeWrapper.getId() + " started.");
    }

    testWorking();
  }

  private void cleanupEnvironment() {
    for (DataNodeWrapper dataNodeWrapper : this.dataNodeWrapperList) {
      dataNodeWrapper.stop();
      dataNodeWrapper.waitingToShutDown();
      dataNodeWrapper.destroyDir();
    }
    for (ConfigNodeWrapper configNodeWrapper : this.configNodeWrapperList) {
      configNodeWrapper.stop();
      configNodeWrapper.waitingToShutDown();
      configNodeWrapper.destroyDir();
    }
    nextTestCase = null;
  }

  public String getTestClassName() {
    StackTraceElement stack[] = Thread.currentThread().getStackTrace();
    for (int i = 0; i < stack.length; i++) {
      String className = stack[i].getClassName();
      if (className.endsWith("IT")) {
        return className.substring(className.lastIndexOf(".") + 1);
      }
    }
    return "UNKNOWN-IT";
  }

  public String getTestClassNameAndDate() {
    Date date = new Date();
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss");
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    for (StackTraceElement stackTraceElement : stack) {
      String className = stackTraceElement.getClassName();
      if (className.endsWith("IT")) {
        return className.substring(className.lastIndexOf(".") + 1) + "-" + formatter.format(date);
      }
    }
    return "IT";
  }

  public void testWorking() throws InterruptedException {
    int counter = 0;
    Thread.sleep(2000);

    do {
      Thread.sleep(1000);

      counter++;
      if (counter > 30) {
        fail("After 30 times retry, the cluster can't work!");
      }

      try (IoTDBConnection connection = getConnection(60);
          Statement statement = connection.createStatement()) {
        statement.execute("SET STORAGE GROUP TO root.test" + counter);
        statement.execute(
            "CREATE TIMESERIES root.test" + counter + ".d0.s0 WITH DATATYPE=INT32, ENCODING=RLE");
        try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES")) {
          if (resultSet.next()) {
            statement.execute("DELETE STORAGE GROUP root.*");
            logger.info("The whole cluster is ready.");
            break;
          }
        }
      } catch (SQLException e) {
        logger.debug(counter + " time(s) connect to cluster failed!", e);
      }
    } while (true);
  }

  @Override
  public void cleanAfterClass() {
    cleanupEnvironment();
  }

  @Override
  public void cleanAfterTest() {
    cleanupEnvironment();
  }

  @Override
  public Connection getConnection() throws SQLException {
    return new ClusterTestConnection(getWriteConnection(null), getReadConnections(null));
  }

  @Override
  public void setNextTestCaseName(String testCaseName) {
    nextTestCase = testCaseName;
  }

  private IoTDBConnection getConnection(int queryTimeout) throws SQLException {
    IoTDBConnection connection =
        (IoTDBConnection)
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX
                    + this.dataNodeWrapperList.get(0).getIp()
                    + ":"
                    + this.dataNodeWrapperList.get(0).getPort(),
                System.getProperty("User", "root"),
                System.getProperty("Password", "root"));
    connection.setQueryTimeout(queryTimeout);

    return connection;
  }

  @Override
  public Connection getConnection(Constant.Version version) throws SQLException {
    return new ClusterTestConnection(getWriteConnection(version), getReadConnections(version));
  }

  protected NodeConnection getWriteConnection(Constant.Version version) throws SQLException {
    DataNodeWrapper dataNode;

    if (System.getProperty("RandomSelectWriteNode", "true").equals("true")) {
      // Randomly choose a node for handling write requests
      dataNode = this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    } else {
      dataNode = this.dataNodeWrapperList.get(0);
    }

    String endpoint = dataNode.getIp() + ":" + dataNode.getPort();
    Connection writeConnection =
        DriverManager.getConnection(
            Config.IOTDB_URL_PREFIX + endpoint + getVersionParam(version),
            System.getProperty("User", "root"),
            System.getProperty("Password", "root"));
    return new NodeConnection(
        endpoint,
        NodeConnection.NodeRole.DATA_NODE,
        NodeConnection.ConnectionRole.WRITE,
        writeConnection);
  }

  protected List<NodeConnection> getReadConnections(Constant.Version version) throws SQLException {
    List<NodeConnection> readConnections = new ArrayList<>();
    for (DataNodeWrapper dataNode : this.dataNodeWrapperList) {
      String endpoint = dataNode.getIp() + ":" + dataNode.getPort();
      Connection readConnection =
          DriverManager.getConnection(
              Config.IOTDB_URL_PREFIX + endpoint + getVersionParam(version),
              System.getProperty("User", "root"),
              System.getProperty("Password", "root"));
      readConnections.add(
          new NodeConnection(
              endpoint,
              NodeConnection.NodeRole.DATA_NODE,
              NodeConnection.ConnectionRole.READ,
              readConnection));
      if (System.getProperty("ReadAndVerifyWithMultiNode", "true").equals("false")) {
        break;
      }
    }

    return readConnections;
  }

  private String getVersionParam(Constant.Version version) {
    if (version == null) {
      return "";
    }
    return "?" + VERSION + "=" + version;
  }
}
