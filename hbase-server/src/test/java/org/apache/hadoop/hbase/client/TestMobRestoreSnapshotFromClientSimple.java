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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.snapshot.MobSnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;

@Tag(LargeTests.TAG)
@Tag(ClientTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: regionReplication={0}")
public class TestMobRestoreSnapshotFromClientSimple
  extends RestoreSnapshotFromClientSimpleTestBase {

  public TestMobRestoreSnapshotFromClientSimple(int numReplicas) {
    super(numReplicas);
  }

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    setupConf(TEST_UTIL.getConfiguration());
    TEST_UTIL.startMiniCluster(3);
  }

  protected static void setupConf(Configuration conf) {
    RestoreSnapshotFromClientTestBase.setupConf(conf);
    TEST_UTIL.getConfiguration().setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
  }

  @Override
  protected void createTable() throws Exception {
    MobSnapshotTestingUtils.createMobTable(TEST_UTIL, tableName, numReplicas, FAMILY);
  }

  @Override
  protected void verifyRowCount(HBaseTestingUtil util, TableName tableName, long expectedRows)
    throws IOException {
    MobSnapshotTestingUtils.verifyMobRowCount(util, tableName, expectedRows);
  }

  @Override
  protected int countRows(Table table, byte[]... families) throws IOException {
    return MobSnapshotTestingUtils.countMobRows(table, families);
  }

  @TestTemplate
  public void testSnapshotAfterRestoringDeletedTable() throws IOException {
    byte[] regularFamily = Bytes.toBytes("regular");
    admin.disableTable(tableName);
    admin.modifyTable(TableDescriptorBuilder.newBuilder(admin.getDescriptor(tableName))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(regularFamily)).build());
    admin.enableTable(tableName);

    byte[] qualifier = Bytes.toBytes("q");
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(Bytes.toBytes("before-restore"));
      put.addColumn(FAMILY, qualifier, Bytes.toBytes("mob-before-restore"));
      put.addColumn(regularFamily, qualifier, Bytes.toBytes("regular-before-restore"));
      table.put(put);
    }
    admin.flush(tableName);
    admin.snapshot(snapshotName2, tableName);

    TEST_UTIL.deleteTable(tableName);
    admin.restoreSnapshot(snapshotName2);

    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(Bytes.toBytes("after-restore"));
      put.addColumn(FAMILY, qualifier, Bytes.toBytes("mob-after-restore"));
      put.addColumn(regularFamily, qualifier, Bytes.toBytes("regular-after-restore"));
      table.put(put);
    }
    admin.flush(tableName);

    String restoredSnapshot = snapshotName2 + "-restored";
    admin.snapshot(restoredSnapshot, tableName);
    assertTrue(admin.listSnapshots().stream()
      .anyMatch(snapshot -> restoredSnapshot.equals(snapshot.getName())));
  }
}
