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
package org.apache.iotdb.integration.env;

import org.apache.iotdb.base.env.BaseEnv;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class EnvUtil {
  private static BaseEnv env;

  static {
    try {
      System.out.println(System.getProperty("TestEnv", "Standalone"));
      switch (System.getProperty("TestEnv", "Standalone")) {
        case "Standalone":
          env =
              (BaseEnv)
                  Class.forName("org.apache.iotdb.db.integration.env.StandaloneEnv").newInstance();
          break;
        case "Remote":
          env =
              (BaseEnv)
                  Class.forName("org.apache.iotdb.integration.env.RemoteServerEnv").newInstance();
          break;
        case "FiveNodeCluster":
          env = (BaseEnv) Class.forName("org.apache.iotdb.db.sql.FiveNodeCusterEnv").newInstance();
          break;
        default:
          throw new ClassNotFoundException("The Property class of TestEnv not found");
      }
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static void init() throws InterruptedException {
    env.initBeforeClass();
  }

  public static void clean() throws IOException {
    env.cleanAfterClass();
  }

  public static Connection getConnection() throws SQLException, ClassNotFoundException {
    return env.getConnection();
  }
}
