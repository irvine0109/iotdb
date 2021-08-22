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
package org.apache.iotdb.db.sql;

import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.jdbc.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.NoProjectNameDockerComposeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class FiveNodeCusterEnv implements BaseEnv {

  private static Logger node1Logger = LoggerFactory.getLogger("iotdb-server_1");
  private static Logger node2Logger = LoggerFactory.getLogger("iotdb-server_2");
  private static Logger node3Logger = LoggerFactory.getLogger("iotdb-server_3");
  private static Logger node4Logger = LoggerFactory.getLogger("iotdb-server_4");
  private static Logger node5Logger = LoggerFactory.getLogger("iotdb-server_5");

  private DockerComposeContainer environment =
      new NoProjectNameDockerComposeContainer(
              "5nodes", new File("../testcontainer/src/test/resources/5nodes/docker-compose.yaml"))
          .withExposedService("iotdb-server_1", 6667, Wait.forListeningPort())
          .withLogConsumer("iotdb-server_1", new Slf4jLogConsumer(node1Logger))
          .withExposedService("iotdb-server_2", 6667, Wait.forListeningPort())
          .withLogConsumer("iotdb-server_2", new Slf4jLogConsumer(node2Logger))
          .withExposedService("iotdb-server_3", 6667, Wait.forListeningPort())
          .withLogConsumer("iotdb-server_3", new Slf4jLogConsumer(node3Logger))
          .withExposedService("iotdb-server_4", 6667, Wait.forListeningPort())
          .withLogConsumer("iotdb-server_4", new Slf4jLogConsumer(node4Logger))
          .withExposedService("iotdb-server_5", 6667, Wait.forListeningPort())
          .withLogConsumer("iotdb-server_5", new Slf4jLogConsumer(node5Logger))
          .withLocalCompose(true);;

  private DockerComposeContainer getContainer() {
    return environment;
  }

  private String getRpcIp() {
    return getContainer().getServiceHost("iotdb-server_1", 6667);
  }

  private int getRpcPort() {
    return getContainer().getServicePort("iotdb-server_1", 6667);
  }

  public Connection getConnection() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    return DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + getRpcIp() + ":" + getRpcPort(), "root", "root");
  }

  private String[] getRpcIps() {
    return new String[] {
      getContainer().getServiceHost("iotdb-server_1", 6667),
      getContainer().getServiceHost("iotdb-server_2", 6667),
      getContainer().getServiceHost("iotdb-server_3", 6667),
      getContainer().getServiceHost("iotdb-server_4", 6667),
      getContainer().getServiceHost("iotdb-server_5", 6667)
    };
  }

  private int[] getRpcPorts() {
    return new int[] {
      getContainer().getServicePort("iotdb-server_1", 6667),
      getContainer().getServicePort("iotdb-server_2", 6667),
      getContainer().getServicePort("iotdb-server_3", 6667),
      getContainer().getServicePort("iotdb-server_4", 6667),
      getContainer().getServicePort("iotdb-server_5", 6667)
    };
  }

  public List<Connection> getConnections() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    List<Connection> connections = new ArrayList<>();
    String[] ips = getRpcIps();
    int[] ports = getRpcPorts();

    for (int i = 0; i < 5; i++) {
      connections.add(
          DriverManager.getConnection(
              Config.IOTDB_URL_PREFIX + ips[i] + ":" + ports[i], "root", "root"));
    }
    return connections;
  }

  public void initBeforeClass() throws InterruptedException {
    environment.start();
  }

  public void cleanAfterClass() {
    environment.close();
  }

  public void initBeforeTest() {}

  public void cleanAfterTest() {}
}
