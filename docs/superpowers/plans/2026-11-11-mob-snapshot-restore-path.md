# MOB Snapshot Restore Path Resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Diagnose and fix the MOB HFile path resolution failure that prevents a second snapshot after restoring a deleted MOB table from a snapshot and writing new MOB data.

**Architecture:** Reproduce the failure in the existing MiniCluster test, capture the exact manifest filename, HFileLink parse groups, resolved table, and generated MOB/archive paths, then choose a narrowly scoped fix based on that evidence. Preserve existing link compatibility and avoid changing non-MOB link naming unless diagnosis proves it necessary.

**Tech Stack:** Java, Apache HBase snapshot/MOB code, JUnit 5, Maven, HBase MiniCluster.

---

## Context and Known Facts

- The reported sequence is: create a MOB table in the `default` namespace, write MOB data, snapshot, delete the table, restore from the snapshot, write more MOB data, and take a second snapshot.
- The second snapshot fails during `SnapshotReferenceUtil.verifyStoreFile` with `CorruptedSnapshotException: Can't find hfile`.
- The observed failing path has a structure like `mobdir/data/t01/<region>-t01/<region>/info/<hfile>`, while the expected path is under `mobdir/data/default/t01/<region>/info/<hfile>`.
- `HFileLink.createHFileLinkName` uses `TableName.getNameAsString()`, which omits `default` from the generated link name.
- `HFileLink.REF_OR_HFILE_LINK_PATTERN` permits an omitted namespace, and the parsed groups are passed to `TableName.valueOf`. An omitted namespace is expected to resolve to the default namespace.
- Therefore, omission of `default` alone does not explain the observed path shape. Confirm the filename, regex groups, resolved `TableName`, and final paths before changing the link format or parser.
- PR `apache/hbase#6290` addresses MOB reference filename date parsing in the cleaner. Its structured-parsing principle may be useful, but its date parser is not part of this snapshot verification failure.
- `hbase-server/src/test/java/org/apache/hadoop/hbase/snapshot/TestMobSnapshotAfterRestore.java` already exists as an untracked reproduction/diagnostic test. Preserve its current contents and inspect its diff before editing it.

## Files and Responsibilities

- `hbase-server/src/main/java/org/apache/hadoop/hbase/snapshot/SnapshotReferenceUtil.java`
  - Capture the filename, link path, parser groups, resolved table, and origin/archive paths at the MOB snapshot verification boundary.
  - Make a production change here only if the evidence points to verification/path construction.
- `hbase-server/src/main/java/org/apache/hadoop/hbase/snapshot/SnapshotManifest.java`
  - If needed, log the MOB store-file name as it enters the snapshot manifest to distinguish producer output from verifier parsing.
- `hbase-server/src/main/java/org/apache/hadoop/hbase/io/HFileLink.java`
  - Change only if evidence proves HFileLink naming or parsing is the faulty layer. Preserve support for existing links with an omitted default namespace.
- `hbase-server/src/main/java/org/apache/hadoop/hbase/mob/MobUtils.java`
  - Change only if evidence identifies MOB table/root path construction as the faulty layer.
- `hbase-server/src/main/java/org/apache/hadoop/hbase/snapshot/RestoreSnapshotHelper.java`
  - Change only if evidence proves the restore path creates a malformed MOB link name.
- `hbase-server/src/main/java/org/apache/hadoop/hbase/regionserver/storefiletracker/StoreFileTrackerBase.java`
  - Change only if evidence proves the tracker creates the malformed link or back-reference. Avoid introducing MOB-specific behavior into its shared path without confirming the affected call site.
- `hbase-server/src/test/java/org/apache/hadoop/hbase/snapshot/TestMobSnapshotAfterRestore.java`
  - Convert the existing diagnostic reproduction into an asserting regression test after the faulty layer is identified.

## Task 1: Capture Exact MOB Verification Inputs and Outputs

**Files:**
- Modify: `hbase-server/src/main/java/org/apache/hadoop/hbase/snapshot/SnapshotReferenceUtil.java`
- Modify if necessary: `hbase-server/src/main/java/org/apache/hadoop/hbase/snapshot/SnapshotManifest.java`

- [ ] **Step 1: Inspect the existing worktree changes**

  Preserve the untracked test and any other pre-existing changes. Do not replace or reset the test file.

- [ ] **Step 2: Add bounded diagnostics at the MOB verification boundary**

  For the failing MOB store file, log:
  - snapshot table and region;
  - `storeFile.getName()` and the resulting `linkPath`;
  - whether the file name is treated as an HFileLink or wrapped as one;
  - all four groups from `HFileLink.REF_OR_HFILE_LINK_PATTERN` when it matches;
  - the `TableName` resolved from the namespace/qualifier groups;
  - `link.getOriginPath()` and `link.getArchivePath()`.

  If the manifest filename differs from the verifier input, add a corresponding diagnostic at `SnapshotManifest.addRegion` for the MOB store-file name. Keep diagnostics at a level appropriate for temporary investigation and remove or reduce them after root cause confirmation.

- [ ] **Step 3: Run the reproducer on Linux**

  From the HBase repository root, run:

  ```sh
  mvn -pl hbase-server -am -Dtest=TestMobSnapshotAfterRestore \
    -Dsurefire.failIfNoSpecifiedTests=false test
  ```

  Expected: the test reproduces the second-snapshot failure and prints the exact input name, parsed groups/table, and generated paths. Do not infer the fix solely from the current exception path.

## Decision Gate: Identify the Faulty Layer Before Implementing a Fix

- [ ] If the parsed table is `default:t01` but the generated path omits or rearranges namespace/table components, trace the MOB root/archive path construction and correct that path construction.
- [ ] If the filename entering the verifier is malformed or contains an unexpected nested link name, trace it back to the component that wrote it and fix that producer.
- [ ] If the parser groups or resolved `TableName` are wrong for a valid filename, correct the structured HFileLink parsing while retaining support for both `t01=...` and `default=t01=...`.
- [ ] If the failure is caused by an incorrect HFileLink generated during restore, correct the restore/link generation path and confirm the StoreFileTracker can read both existing and newly generated links.
- [ ] Do not add an explicit `default=` prefix merely to make the failing path look right. First demonstrate that the current name is being interpreted incorrectly and that the proposed format resolves to the intended path.
- [ ] Do not use `indexOf("-")` to split a link name. If parsing is implicated, use the existing structured link pattern or an equally explicit parser.

## Task 2: Implement the Evidence-Based Minimal Fix

**Files:** Select only the file or files implicated by the decision gate above. Do not change global link naming or unrelated MOB cleaner date parsing.

- [ ] **Step 1: Add a focused failing assertion to the regression test**

  The test must assert that the full sequence succeeds, including creation of the second snapshot. Also verify the first and second snapshots with `SnapshotReferenceUtil.verifySnapshot`; do not swallow verification errors as a successful test outcome.

- [ ] **Step 2: Run the focused test and confirm it fails for the reported reason**

  Use the Linux Maven command from Task 1. Confirm the failure occurs at the expected snapshot/path check rather than during cluster setup or unrelated MOB operations.

- [ ] **Step 3: Make the smallest production change supported by the diagnostics**

  Preserve compatibility with existing HFileLinks and keep non-MOB behavior unchanged unless the evidence establishes that a shared parser is the faulty component.

- [ ] **Step 4: Re-run the regression test**

  Expected: both snapshots verify successfully after the restore-and-write cycle, with no missing MOB HFile path.

## Task 3: Cover Namespace and Link-Parsing Compatibility

**Files:**
- Modify: `hbase-server/src/test/java/org/apache/hadoop/hbase/snapshot/TestMobSnapshotAfterRestore.java`
- Add or modify a focused HFileLink test only if the production parser or link-name format changes.

- [ ] **Step 1: Keep the default-namespace MOB restore cycle as the required regression**

  Assert first snapshot, delete, restore, second MOB write/flush, second snapshot, and verification of both snapshots.

- [ ] **Step 2: Add a named-namespace MOB comparison**

  Confirm the existing named-namespace behavior remains successful through the equivalent sequence.

- [ ] **Step 3: If parsing or link-name generation changed, test old and new representations**

  Verify that a default-namespace link without an explicit `default=` prefix remains readable. If the fix introduces an explicit prefix, verify that both the old omitted-prefix form and the explicit-prefix form resolve to `default:<table>`.

- [ ] **Step 4: If delimiter parsing changed, include a table qualifier containing `-`**

  Ensure the parser identifies the region and HFile components structurally rather than splitting at a dash inside the table qualifier.

- [ ] **Step 5: Run the targeted tests**

  From the repository root on Linux:

  ```sh
  mvn -pl hbase-server -am \
    -Dtest=TestMobSnapshotAfterRestore,TestHFileLink \
    -Dsurefire.failIfNoSpecifiedTests=false test
  ```

  Run `TestHFileLink` when the production change affects HFileLink parsing or name generation. If it does not, run only `TestMobSnapshotAfterRestore`.

## Task 4: Remove Temporary Diagnostics and Review the Final Diff

**Files:** All files modified in the preceding tasks.

- [ ] **Step 1: Remove temporary logging that is not needed for normal operation**

  Keep only durable diagnostics that provide ongoing operational value and follow existing logging conventions.

- [ ] **Step 2: Review compatibility and scope**

  Confirm:
  - non-MOB snapshot/link behavior is unchanged;
  - existing MOB links without an explicit default namespace remain readable;
  - restore, snapshot verification, and StoreFileTracker agree on the link representation;
  - the fix does not alter MOB cleaner date parsing unless a separate, verified requirement calls for it.

- [ ] **Step 3: Review the final diff and test results**

  Check that only the evidence-supported production change, the regression coverage, and directly related diagnostics/documentation remain.

## Completion Criteria

- The root cause is demonstrated by the captured input filename, regex groups, resolved table, and generated path.
- The default-namespace MOB restore/write/second-snapshot regression passes.
- Both snapshots verify after the sequence.
- Named-namespace behavior remains covered and passing.
- Existing omitted-default-namespace HFileLinks remain readable.
- No speculative global HFileLink format change or unrelated PR #6290 cleaner/date-parser change is included.
