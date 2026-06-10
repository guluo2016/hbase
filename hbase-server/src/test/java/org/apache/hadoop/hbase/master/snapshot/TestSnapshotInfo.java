package org.apache.hadoop.hbase.master.snapshot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotInfo;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import java.io.IOException;
import java.util.List;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestSnapshotInfo {

  private final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  

  private Path rootDir;
  private FileSystem fs;
  private Configuration conf;
  private Admin admin;
  private String currentTestName;

  @BeforeEach
  public void setup(TestInfo testInfo) throws Exception {
    TEST_UTIL.startMiniCluster(1);
    rootDir = TEST_UTIL.getDataTestDir();
    fs = TEST_UTIL.getTestFileSystem();
    conf = TEST_UTIL.getConfiguration();
    admin = TEST_UTIL.getAdmin();
    currentTestName = testInfo.getTestMethod().get().getName();
  }

  @AfterEach
  public void tearDown() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.getTestFileSystem().delete(TEST_UTIL.getDataTestDir(), true);
  }

  @Test
  public void testGetSnapshotList() throws IOException {
    Path snapshotDir = SnapshotDescriptionUtils.getSnapshotsDir(rootDir);

    // Now,
    assertFalse(fs.exists(snapshotDir));
    List<SnapshotDescription> snapshotDescList = SnapshotInfo.getSnapshotList(conf);
    assertTrue(snapshotDescList.isEmpty());

    TableName tableName = TableName.valueOf(currentTestName);
    TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
    admin.createTable(tableDescriptorBuilder.build());
    assertTrue(admin.tableExists(tableName));
    String snapshotName = "snapshot_" + currentTestName;
    admin.snapshot(snapshotName, tableName);

    assertTrue(fs.exists(snapshotDir));
    snapshotDescList = SnapshotInfo.getSnapshotList(conf);
    assertFalse(snapshotDescList.isEmpty());

    admin.deleteSnapshot(snapshotName);
    assertTrue(fs.exists(snapshotDir));
    snapshotDescList = SnapshotInfo.getSnapshotList(conf);
    assertTrue(snapshotDescList.isEmpty());
  }

}
