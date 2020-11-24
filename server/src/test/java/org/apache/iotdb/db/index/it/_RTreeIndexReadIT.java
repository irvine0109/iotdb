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
package org.apache.iotdb.db.index.it;

import static org.apache.iotdb.db.index.IndexTestUtils.getArrayRange;
import static org.apache.iotdb.db.index.common.IndexType.ELB_INDEX;
import static org.apache.iotdb.db.index.common.IndexType.RTREE_PAA;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.index.IndexManager;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.math.Randomwalk;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class _RTreeIndexReadIT {

  private static final String insertPattern = "INSERT INTO %s(timestamp, %s) VALUES (%d, %.3f)";

  private static final String storageGroupSub = "root.wind1";
  private static final String storageGroupWhole = "root.wind2";

  private static final String speed1 = "root.wind1.azq01.speed";
  private static final String speed1Device = "root.wind1.azq01";
  private static final String speed1Sensor = "speed";

  private static final String directionDevicePattern = "root.wind2.%d";
  private static final String directionPattern = "root.wind2.%d.direction";
  private static final String directionSensor = "direction";

  private static final String indexSub = speed1;
  private static final String indexWhole = "root.wind2.*.direction";
  private static final int wholeSize = 20;
  private static final int wholeDim = 15;
  private static final int PAA_Dim = 4;
  private static final int subLength = 50;

  @Before
  public void setUp() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(true);
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    insertSQL();
  }

  private void insertSQL() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
//    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(false);
    try (Connection connection = DriverManager.getConnection
        (Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement();) {
//      statement.execute(String.format("SET STORAGE GROUP TO %s", storageGroupSub));
      statement.execute(String.format("SET STORAGE GROUP TO %s", storageGroupWhole));

      for (int i = 0; i < 1; i++) {
        String wholePath = String.format(directionPattern, i);
//        System.out.println(String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", wholePath));
        statement.execute(
            String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", wholePath));
      }
      statement.execute(
          String.format(
              "CREATE INDEX ON %s WITH INDEX=%s, SERIES_LENGTH=%d, FEATURE_DIM=%d, MAX_ENTRIES=%d, MIN_ENTRIES=%d",
              indexWhole, RTREE_PAA, wholeDim, PAA_Dim, 10, 2));

//      TVList wholeInput = Randomwalk.generateRanWalkTVList(wholeDim * wholeSize);
//      TVList wholeInput = IndexUtils.;
      for (int i = 0; i < wholeSize; i++) {
        String device = String.format(directionDevicePattern, i);
//        System.out.println(String.format(insertPattern, device, directionSensor, wholeInput.getTime(i), wholeInput.getDouble(i)));
        for (int j = 0; j < wholeDim; j++) {
          statement.execute(String.format(insertPattern,
              device, directionSensor, j, (i * wholeDim + j) * 1d));
        }
      }
      statement.execute("flush");
      System.out.println(IndexManager.getInstance().getRouter());
      System.out.println(IndexManager.getInstance().getRouter());
//      Assert.assertEquals(
//          "<{ELB_INDEX=[type: ELB_INDEX, time: 0, props: {BLOCK_SIZE=10}]},root.wind1.azq01.speed: {ELB_INDEX=[0-9:45.00, 10-19:145.00, 20-29:245.00, 30-39:345.00, 40-49:445.00]}>;",
//          IndexManager.getInstance().getRouter().toString());

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(false);
  }


  @Test
  public void checkRead() throws ClassNotFoundException {

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager.
        getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String querySQL = String
          .format("SELECT TOP 2 direction FROM root.wind2.* WHERE direction LIKE (%s)",
              getArrayRange(120, 120 + wholeDim));

      System.out.println(querySQL);
      boolean hasIndex = statement.execute(querySQL);
      String[] gt = {"0,0,50210,0,50219,0,0.0"};
      Assert.assertTrue(hasIndex);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder sb = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            sb.append(resultSet.getString(i)).append(",");
          }
          System.out.println(sb);
//          Assert.assertEquals(gt[cnt], builder.toString());
          cnt++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

}
