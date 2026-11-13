<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# MOB 快照恢复后再次快照失败修复方案

## 状态

方案已与用户确认，代码修改已进行；测试验证尚未完成。

## 问题描述

问题发生在包含 MOB 数据的表完成快照、删除原表、从快照恢复、再次写入数据并创建第二个快照时。第二个快照在校验阶段失败：已有的 MOB 文件链接被误认为普通 StoreFile 文件名，校验逻辑又为它套了一层 HFileLink 名称，最终查找了错误路径。

补上 MOB link 识别后，校验可以找到 link 指向的实际文件，但会进一步报告 manifest 中的预期大小为 `0`、实际文件大小非零。原因是 StoreFileTracker 没有把这种 MOB link 建成带底层文件引用的 `StoreFileInfo`；manifest 因而记录了文件系统中空 link 文件的长度，而不是 MOB HFile 的长度。

问题涉及指向 MOB 文件的 MOB link。MOB 文件名在下划线后包含 encoded region，而普通 HFile 文件名规则只接受十六进制文件名及少量特定后缀。因此，`HFileLink.isHFileLink` 无法识别这类 MOB link。现有的 `StoreFileInfo.isMobFileLink` 使用更宽泛的 link 格式进行识别，并已用于快照恢复流程。

命名空间不是该分类错误的根因。宽泛的 link 格式允许省略命名空间；`TableName.valueOf` 会将缺失的命名空间归一化为默认命名空间。因此，带命名空间和不带命名空间的 MOB link 都需要覆盖。

## 相关代码路径

- [`SnapshotReferenceUtil.verifyStoreFile`](../../hbase-server/src/main/java/org/apache/hadoop/hbase/snapshot/SnapshotReferenceUtil.java)：判断 manifest 中的文件名是否已经是 link，或是否需要生成新 link 名称。目前只通过 `HFileLink.isHFileLink` 检查普通文件名。
- [`StoreFileInfo.isMobFileLink`](../../hbase-server/src/main/java/org/apache/hadoop/hbase/regionserver/StoreFileInfo.java)：使用更宽泛的 HFileLink 格式识别 MOB link。
- [`StoreFileTrackerBase.getStoreFileInfo`](../../hbase-server/src/main/java/org/apache/hadoop/hbase/regionserver/storefiletracker/StoreFileTrackerBase.java)：将文件名转换为 `StoreFileInfo`。目前没有把 MOB link 作为 link 初始化，导致其大小来自空的 link 文件。
- [`SnapshotManifestV2.storeFile`](../../hbase-server/src/main/java/org/apache/hadoop/hbase/snapshot/SnapshotManifestV2.java)：对 `StoreFileInfo.isLink()` 为真的文件记录被引用文件的大小；否则记录 `StoreFileInfo.getSize()`。
- [`RestoreSnapshotHelper.restoreStoreFile`](../../hbase-server/src/main/java/org/apache/hadoop/hbase/snapshot/RestoreSnapshotHelper.java)：恢复快照时已经同时识别普通 HFileLink 和 MOB link。
- [`SnapshotReferenceUtil.getHFileNames`](../../hbase-server/src/main/java/org/apache/hadoop/hbase/snapshot/SnapshotReferenceUtil.java)：提取快照引用的文件名，供快照文件保留逻辑使用。目前其 link 分支也只检查 `HFileLink.isHFileLink`。
- [`SnapshotHFileCleaner`](../../hbase-server/src/main/java/org/apache/hadoop/hbase/master/snapshot/SnapshotHFileCleaner.java)：使用 `getHFileNames` 返回的文件名集合。

## 修改方案

### 1. 快照校验时保留已有 MOB link

修改 `SnapshotReferenceUtil.verifyStoreFile`：

1. 保持现有的 reference 文件处理逻辑不变。
2. 判断如何构造 `linkPath` 时，若 `HFileLink.isHFileLink(fileName)` 或 `StoreFileInfo.isMobFileLink(fileName)` 任一返回 `true`，就将该文件名视为已有 link。
3. 对已有 link，直接使用它在 family 目录下的现有文件名；不要再对该名称调用 `HFileLink.createHFileLinkName`，否则会重复添加 table/region 前缀。
4. 保持现有的 link 查找和文件大小校验逻辑不变，尤其保留 MOB region 对应的 MOB root 和 archive 路径选择。

这是针对快照校验的局部修改，不改变普通 HFileLink 的全局定义。

### 2. 在 StoreFileTracker 中将 MOB link 表示为 link

修改 `StoreFileTrackerBase.getStoreFileInfo`：

1. 在普通 `HFileLink.isHFileLink` 检查之外，增加 `StoreFileInfo.isMobFileLink` 检查。
2. 对 MOB link 使用 `HFileLink.buildFromHFileLinkPattern` 创建 link，并将其保存在返回的 `StoreFileInfo` 中。
3. 不再把 MOB link 文件系统状态中的 `0` 字节长度当作底层 HFile 的大小。

这样 `SnapshotManifestV2.storeFile` 会通过 `getReferencedFileStatus` 读取实际 MOB HFile 的大小，写入 manifest；验证阶段比较的预期大小与实际目标文件大小一致。

### 3. 将 MOB link 的目标文件纳入快照文件名集合

修改 `SnapshotReferenceUtil.getHFileNames`：

1. 除普通 HFileLink 外，也识别 MOB link。
2. 使用已有的 `HFileLink.getReferencedHFileName` 解析器，将底层 MOB 文件名加入返回集合，而不是把完整的复合 link 名当作目标文件名加入。
3. 普通 HFile 和 reference 文件的处理保持不变。

这样可以使快照文件保留逻辑与校验逻辑一致：快照文件集合应记录 link 实际引用的文件。由于该集合由 `SnapshotHFileCleaner` 使用，回归测试应单独覆盖此 helper，而不只验证快照校验流程。

### 4. 增加回归测试

从现有 MOB 快照测试入手：

- [`TestMobRestoreSnapshotHelper`](../../hbase-server/src/test/java/org/apache/hadoop/hbase/snapshot/TestMobRestoreSnapshotHelper.java) 已覆盖删除 MOB 表并从快照恢复的场景。可在此基础上增加或调整“恢复后继续写入数据并创建第二个快照”的场景。
- 在 `TestMobRestoreSnapshotHelper` 的集成场景中验证 `getHFileNames` 返回底层 MOB 文件名，并通过 `SnapshotHFileCleaner` 确认该文件不会被列为可删除文件。
- [`TestSnapshotHFileCleaner`](../../hbase-server/src/test/java/org/apache/hadoop/hbase/master/snapshot/TestSnapshotHFileCleaner.java) 继续作为普通文件快照保留行为的回归覆盖。

集成测试场景应包含：

1. 创建同时包含普通数据和 MOB 数据的 MOB 表。
2. 创建快照并删除原表。
3. 从该快照恢复表。
4. 再写入普通数据和 MOB 数据，并根据测试需要确保数据已 flush。
5. 创建第二个快照，并断言快照成功完成。
6. 分别覆盖默认命名空间表和自定义命名空间表。
7. 断言已有 MOB link 没有被再次包装，且其引用文件能从 MOB root 或 archive 路径解析到。
8. 断言二次快照 manifest 中 MOB link 对应的 `fileSize` 是底层 MOB HFile 的大小，而不是空 link 的 `0`。

还应保留普通 HFile、普通 HFileLink 和 reference 文件的回归覆盖，确保新增 MOB link 识别逻辑不改变这些文件的现有行为。

## 非目标

- 不放宽 `HFileLink.isHFileLink` 的规则，也不修改 `StoreFileInfo.HFILE_NAME_REGEX`。
- 不改变 HFileLink 命名、MOB 文件命名、MOB 存储布局或 snapshot manifest 格式。
- 不修改现有快照恢复/克隆实现；该实现已经能够识别 MOB link。
- 不重构无关的快照或 cleaner 代码。

## 验收标准

- 带命名空间和不带命名空间的“恢复、修改、创建第二个快照”流程都成功。
- 校验逻辑查找原始 MOB link 的目标，而不是重复包装后的文件名。
- manifest 记录的 MOB link 文件大小与被引用的 MOB HFile 大小一致。
- `getHFileNames` 返回 MOB link 引用的底层文件名，供快照保留逻辑使用。
- 普通 HFile、HFileLink 和 reference 文件的现有测试继续通过。

## 验证计划

先运行定向 MOB 恢复/快照测试和 snapshot HFile cleaner 测试，再运行相关的 `hbase-server` 快照测试。集成场景必须同时验证两种命名空间形式；仅断言宽松的文件名解析器能够接受该字符串，不足以证明问题已修复。
