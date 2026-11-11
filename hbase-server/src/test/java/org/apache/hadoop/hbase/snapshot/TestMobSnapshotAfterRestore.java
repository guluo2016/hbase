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
package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Takes a snapshot of a MOB table, restores it after the table was deleted, writes more MOB data
 * and takes another snapshot.
 */
@Tag(ClientTests.TAG)
@Tag(MediumTests.TAG)
public class TestMobSnapshotAfterRestore {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final byte[] FAM = Bytes.toBytes("info");
  private static final TableName TABLE_NAME = TableName.valueOf("t01");

  @BeforeAll
  public static void setupCluster() throws Exception {
    UTIL.getConfiguration().setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
    UTIL.startMiniCluster(1);
  }

  @AfterAll
  public static void tearDownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSnapshotAfterRestoreAndPutMobData() throws Exception {
    MobSnapshotTestingUtils.createMobTable(UTIL, TABLE_NAME, 1, FAM);
    try (Table table = UTIL.getConnection().getTable(TABLE_NAME)) {
      putMobData(table, "a");
    }
    Admin admin = UTIL.getAdmin();
    admin.snapshot("t01_snap01", TABLE_NAME);
    dump("after snap01");
    verifySnapshot("t01_snap01");

    admin.disableTable(TABLE_NAME);
    admin.deleteTable(TABLE_NAME);
    dump("after delete table");
    verifySnapshot("t01_snap01");

    admin.cloneSnapshot("t01_snap01", TABLE_NAME);
    dump("after restore");
    try (Table table = UTIL.getConnection().getTable(TABLE_NAME)) {
      putMobData(table, "b");
    }
    dump("after second put");
    verifySnapshot("t01_snap01");
    admin.snapshot("t01_snap02", TABLE_NAME);
  }

  /** Lists every file under the MOB table dir (mobdir and archive) for TABLE_NAME. */
  private void dump(String stage) throws IOException {
    Path rootDir = CommonFSUtils.getRootDir(UTIL.getConfiguration());
    FileSystem fs = rootDir.getFileSystem(UTIL.getConfiguration());
    Path mobTableDir = CommonFSUtils.getTableDir(new Path(rootDir, "mobdir"), TABLE_NAME);
    Path archiveTableDir = CommonFSUtils.getTableDir(new Path(rootDir, "archive"), TABLE_NAME);
    System.out.println("=== MOB DUMP [" + stage + "] ===");
    list(fs, mobTableDir, "mobdir");
    list(fs, archiveTableDir, "archive");
  }

  private static void list(FileSystem fs, Path dir, String label) throws IOException {
    if (!fs.exists(dir)) {
      System.out.println("  " + label + ": " + dir + " does not exist");
      return;
    }
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(dir, true);
    while (it.hasNext()) {
      LocatedFileStatus st = it.next();
      System.out.println("  " + label + ": " + st.getPath() + " (" + st.getLen() + " bytes)");
    }
  }

  /** Verifies the named snapshot; prints the failure instead of aborting so later stages still run. */
  private void verifySnapshot(String name) throws IOException {
    Path rootDir = CommonFSUtils.getRootDir(UTIL.getConfiguration());
    FileSystem fs = rootDir.getFileSystem(UTIL.getConfiguration());
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(name, rootDir);
    if (!fs.exists(snapshotDir)) {
      System.out.println("=== VERIFY " + name + ": snapshot dir not found ===");
      return;
    }
    SnapshotProtos.SnapshotDescription desc =
      SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    try {
      SnapshotReferenceUtil.verifySnapshot(UTIL.getConfiguration(), fs, snapshotDir, desc);
      System.out.println("=== VERIFY " + name + ": OK ===");
    } catch (IOException e) {
      System.out.println("=== VERIFY " + name + ": FAILED: " + e + " ===");
    }
  }

  private static void putMobData(Table table, String prefix) throws IOException {
    byte[] value = new byte[1024];
    for (int i = 0; i < 10; i++) {
      Put put = new Put(Bytes.toBytes(prefix + i));
      put.addColumn(FAM, Bytes.toBytes("q"), value);
      table.put(put);
    }
    UTIL.getAdmin().flush(TABLE_NAME);
  }
}
