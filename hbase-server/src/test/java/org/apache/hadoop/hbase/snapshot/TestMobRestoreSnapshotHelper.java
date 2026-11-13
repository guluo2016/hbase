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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.snapshot.SnapshotHFileCleaner;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.snapshot.MobSnapshotTestingUtils.SnapshotMock;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;

/**
 * Test the restore/clone operation from a file-system point of view.
 */
@Tag(MediumTests.TAG)
public class TestMobRestoreSnapshotHelper extends TestRestoreSnapshotHelper {

  final Logger LOG = LoggerFactory.getLogger(getClass());

  @Override
  protected void setupConf(Configuration conf) {
    conf.setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
  }

  @Override
  protected SnapshotMock createSnapshotMock() throws IOException {
    return new SnapshotMock(TEST_UTIL.getConfiguration(), fs, rootDir);
  }

  @Override
  protected void createTableAndSnapshot(TableName tableName, String snapshotName)
    throws IOException {
    byte[] column = Bytes.toBytes("A");
    Table table = MobSnapshotTestingUtils.createMobTable(TEST_UTIL, tableName, column);
    TEST_UTIL.loadTable(table, column);
    TEST_UTIL.getAdmin().snapshot(snapshotName, tableName);
  }

  @Test
  public void testRestoreMobTableFromSnapshot() throws IOException {
    TableName mobTableName = TableName.valueOf("testRestoreMobTable");
    String snapshotName = "testRestoreMobTable_snapshot";
    createTableAndSnapshot(mobTableName, snapshotName);
    assertTrue(TEST_UTIL.getAdmin().tableExists(mobTableName));
    assertTrue(MobUtils.hasMobColumns(TEST_UTIL.getAdmin().getDescriptor(mobTableName)));
    assertTrue(TEST_UTIL.getAdmin().listSnapshots().stream()
      .anyMatch(snapshotDescription -> snapshotName.equals(snapshotDescription.getName())));

    // Clone the snapshot to a new MOB table
    TableName newMobTableName = TableName.valueOf("newTestRestoreMobTable");
    TEST_UTIL.getAdmin().cloneSnapshot(snapshotName, newMobTableName);
    assertTrue(TEST_UTIL.getAdmin().tableExists(newMobTableName));
    assertTrue(MobUtils.hasMobColumns(TEST_UTIL.getAdmin().getDescriptor(newMobTableName)));

    Path hbaseRootDir = TEST_UTIL.getDefaultRootDirPath();
    Path mobRegionPath = MobUtils.getMobTableDir(hbaseRootDir, newMobTableName);
    assertTrue(fs.exists(mobRegionPath));
    Path errorMobRegionPathInTableDir =
      new Path(CommonFSUtils.getTableDir(hbaseRootDir, newMobTableName),
        MobUtils.getMobRegionInfo(newMobTableName).getEncodedName());
    assertFalse(fs.exists(errorMobRegionPathInTableDir));

    try (Table originMobTable = TEST_UTIL.getConnection().getTable(mobTableName);
      Table clonedMobTable = TEST_UTIL.getConnection().getTable(newMobTableName)) {
      assertEquals(HBaseTestingUtil.countRows(originMobTable),
        HBaseTestingUtil.countRows(clonedMobTable));
    }

    // Delete the original MOB table and restore it from the snapshot
    TEST_UTIL.deleteTable(mobTableName);
    assertFalse(TEST_UTIL.getAdmin().tableExists(mobTableName));
    TEST_UTIL.getAdmin().cloneSnapshot(snapshotName, mobTableName);
    assertTrue(TEST_UTIL.getAdmin().tableExists(mobTableName));
    assertTrue(MobUtils.hasMobColumns(TEST_UTIL.getAdmin().getDescriptor(mobTableName)));
    mobRegionPath = MobUtils.getMobTableDir(hbaseRootDir, mobTableName);
    assertTrue(fs.exists(mobRegionPath));
    errorMobRegionPathInTableDir = new Path(CommonFSUtils.getTableDir(hbaseRootDir, mobTableName),
      MobUtils.getMobRegionInfo(mobTableName).getEncodedName());
    assertFalse(fs.exists(errorMobRegionPathInTableDir));
  }

  @Test
  public void testSecondSnapshotAfterMobRestore() throws IOException {
    testSecondSnapshotAfterMobRestore(TableName.valueOf("testSecondSnapshotAfterMobRestore"), "testSecondSnapshotAfterMobRestore_snapshot", "testSecondSnapshotAfterMobRestore_snapshot2");
  }

  @Test
  public void testSecondSnapshotAfterMobTableRestoreWithNamespace() throws IOException {
    String namespace = "testMobSnapshotNs";
    TEST_UTIL.getAdmin().createNamespace(NamespaceDescriptor.create(namespace).build());
    try {
      testSecondSnapshotAfterMobRestore(TableName.valueOf(namespace, "testSecondSnapshotAfterMobRestore"), "testSecondSnapshotAfterMobRestoreWithNamespace_snapshot", "testSecondSnapshotAfterMobRestoreWithNamespace_snapshot2");
    } finally {
      TEST_UTIL.getAdmin().deleteNamespace(namespace);
    }
  }

  private void testSecondSnapshotAfterMobRestore(TableName tableName, String snapshotName,
    String secondSnapshotName) throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    byte[] mobFamily = Bytes.toBytes("A");
    byte[] regularFamily = Bytes.toBytes("B");
    try {
      admin.createTable(TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(mobFamily).setMobEnabled(true)
          .setMobThreshold(0L).build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(regularFamily)).build());
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
        table.put(new Put(Bytes.toBytes("before-restore"))
          .addColumn(mobFamily, Bytes.toBytes("q"), Bytes.toBytes("mob value"))
          .addColumn(regularFamily, Bytes.toBytes("q"), Bytes.toBytes("regular value")));
      }
      admin.flush(tableName);
      admin.snapshot(snapshotName, tableName);

      TEST_UTIL.deleteTable(tableName);
      admin.cloneSnapshot(snapshotName, tableName);
      try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
        table.put(new Put(Bytes.toBytes("after-restore"))
          .addColumn(mobFamily, Bytes.toBytes("q"), Bytes.toBytes("mob value"))
          .addColumn(regularFamily, Bytes.toBytes("q"), Bytes.toBytes("regular value")));
      }
      admin.flush(tableName);
      admin.snapshot(secondSnapshotName, tableName);

      Path snapshotDir =
        SnapshotDescriptionUtils.getCompletedSnapshotDir(secondSnapshotName, rootDir);
      Set<String> snapshotFileNames = SnapshotReferenceUtil.getHFileNames(conf, fs, snapshotDir);
      assertMobLinkSizesMatchTarget(snapshotDir);
      Path mobFamilyPath = MobUtils.getMobFamilyPath(
        MobUtils.getMobRegionPath(rootDir, tableName), Bytes.toString(mobFamily));
      boolean foundMobLink = false;
      FileStatus referencedMobFile = null;
      for (FileStatus fileStatus : fs.listStatus(mobFamilyPath)) {
        String fileName = fileStatus.getPath().getName();
        if (StoreFileInfo.isMobFileLink(fileName)) {
          foundMobLink = true;
          assertFalse(HFileLink.isHFileLink(fileName));
          assertTrue(snapshotFileNames.contains(HFileLink.getReferencedHFileName(fileName)));
          assertFalse(snapshotFileNames.contains(fileName));
          HFileLink link = HFileLink.buildFromHFileLinkPattern(
            MobUtils.getQualifiedMobRootDir(conf), HFileArchiveUtil.getArchivePath(conf),
            new Path(Bytes.toString(mobFamily), fileName));
          referencedMobFile = link.getFileStatus(fs);
        }
      }
      assertTrue(foundMobLink);
      assertTrue(referencedMobFile != null);

      SnapshotHFileCleaner cleaner = new SnapshotHFileCleaner();
      cleaner.setConf(conf);
      cleaner.init(Collections.singletonMap(HMaster.MASTER, TEST_UTIL.getHBaseCluster().getMaster()));
      try {
        assertFalse(
          cleaner.getDeletableFiles(Collections.singletonList(referencedMobFile)).iterator()
            .hasNext());
      } finally {
        cleaner.stop("test complete");
      }
    } finally {
      if (admin.tableExists(tableName)) {
        TEST_UTIL.deleteTable(tableName);
      }
      deleteSnapshotIfExists(admin, secondSnapshotName);
      deleteSnapshotIfExists(admin, snapshotName);
    }
  }

  private void assertMobLinkSizesMatchTarget(Path snapshotDir) throws IOException {
    SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    boolean[] foundMobLink = { false };
    SnapshotReferenceUtil.visitTableStoreFiles(conf, fs, snapshotDir, snapshotDesc,
      new SnapshotReferenceUtil.StoreFileVisitor() {
        @Override
        public void storeFile(RegionInfo regionInfo, String family,
          SnapshotRegionManifest.StoreFile storeFile) throws IOException {
          if (MobUtils.isMobRegionInfo(regionInfo)
            && StoreFileInfo.isMobFileLink(storeFile.getName())) {
            foundMobLink[0] = true;
            HFileLink link = HFileLink.buildFromHFileLinkPattern(
              MobUtils.getQualifiedMobRootDir(conf), HFileArchiveUtil.getArchivePath(conf),
              new Path(family, storeFile.getName()));
            long targetSize = link.getFileStatus(fs).getLen();
            assertTrue(storeFile.hasFileSize());
            assertTrue(targetSize > 0);
            assertEquals(targetSize, storeFile.getFileSize());
          }
        }
      });
    assertTrue(foundMobLink[0]);
  }

  private void deleteSnapshotIfExists(Admin admin, String snapshotName) throws IOException {
    if (admin.listSnapshots().stream()
      .anyMatch(snapshot -> snapshotName.equals(snapshot.getName()))) {
      admin.deleteSnapshot(snapshotName);
    }
  }
}
