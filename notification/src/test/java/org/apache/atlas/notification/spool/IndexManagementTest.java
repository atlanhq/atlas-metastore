/**
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
package org.apache.atlas.notification.spool;

import org.apache.atlas.notification.spool.models.IndexRecord;
import org.apache.atlas.notification.spool.models.IndexRecords;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IndexManagementTest extends BaseTest {
    @Test
    public void fileNameGeneration() {
        String handlerName = "someHandler";
        SpoolConfiguration cfg = getSpoolConfiguration(spoolDir, handlerName);

        IndexRecord record = new IndexRecord(StringUtils.EMPTY);
        Assertions.assertEquals(SpoolUtils.getIndexFileName(cfg.getSourceName(), cfg.getMessageHandlerName()), "index-test-src-someHandler.json");
        Assertions.assertTrue(SpoolUtils.getSpoolFileName(cfg.getSourceName(), cfg.getMessageHandlerName(), record.getId()).startsWith("spool-test-src-someHandler-"));
    }

    @Test
    public void verifyLoad() throws IOException {
        final int expectedRecords = 2;
        SpoolConfiguration cfg = getSpoolConfiguration();

        IndexManagement.IndexFileManager indexFileManager = new IndexManagement.IndexFileManager(SOURCE_TEST, cfg.getIndexFile(), cfg.getIndexDoneFile(), null, 2);

        Assertions.assertEquals(indexFileManager.getRecords().size(), expectedRecords);

        Assertions.assertEquals(indexFileManager.getRecords().get(0).getId(), "1");
        Assertions.assertEquals(indexFileManager.getRecords().get(1).getId(), "2");
    }

    @Test
    public void addAndRemove() throws IOException {
        File newIndexFile = getNewIndexFile('3');
        File newIndexDoneFile = getNewIndexDoneFile('3');

        IndexManagement.IndexFileManager indexFileManager = new IndexManagement.IndexFileManager(SOURCE_TEST, newIndexFile, newIndexDoneFile, null, 2);

        int expectedCount = 2;
        Assertions.assertEquals(indexFileManager.getRecords().size(), expectedCount);

        IndexRecord r3 = indexFileManager.add("3.log");
        IndexRecord r4 = indexFileManager.add("4.log");
        IndexRecord r5 = indexFileManager.add("5.log");

        r4.updateFailedAttempt();
        indexFileManager.updateIndex(r4);

        r5.setLine(100);
        indexFileManager.updateIndex(r5);

        IndexRecords records = indexFileManager.loadRecords(newIndexFile);
        Assertions.assertTrue(records.getRecords().containsKey(r3.getId()));
        Assertions.assertTrue(records.getRecords().containsKey(r4.getId()));
        Assertions.assertTrue(records.getRecords().containsKey(r5.getId()));

        Assertions.assertEquals(records.getRecords().get(r3.getId()).getStatus(), r3.getStatus());
        Assertions.assertEquals(records.getRecords().get(r4.getId()).getFailedAttempt(), r4.getFailedAttempt());
        Assertions.assertEquals(records.getRecords().get(r5.getId()).getLine(), r5.getLine());

        indexFileManager.remove(r3);
        indexFileManager.remove(r4);
        indexFileManager.remove(r5);

        Assertions.assertEquals(indexFileManager.getRecords().size(), expectedCount);
    }

    @Test
    public void verifyOperations() throws IOException {
        SpoolConfiguration cfg = getSpoolConfigurationTest();

        File newIndexFile = getNewIndexFile('2');
        File newIndexDoneFile = getNewIndexDoneFile('2');

        File archiveDir = cfg.getArchiveDir();
        IndexManagement.IndexFileManager indexFileManager = new IndexManagement.IndexFileManager(SOURCE_TEST, newIndexFile, newIndexDoneFile, null, 2);

        verifyAdding(indexFileManager);
        verifySaveAndLoad(indexFileManager);
        verifyRemove(indexFileManager);
        verifyRecords(indexFileManager);

        checkDoneFile(newIndexDoneFile, archiveDir, 2, "5.log");

        verifyArchiving(indexFileManager);
    }

    private void verifyRecords(IndexManagement.IndexFileManager indexFileManager) {
        List<IndexRecord> records = indexFileManager.getRecords();

        Assertions.assertEquals(records.size(), 5);
        Assertions.assertTrue(records.get(3).getPath().endsWith("3.log"));
        Assertions.assertEquals(records.get(3).getStatus(), IndexRecord.STATUS_WRITE_IN_PROGRESS);
        Assertions.assertEquals(records.get(2).getFailedAttempt(), 0);
        Assertions.assertEquals(records.get(1).getDoneCompleted(), 0);
        Assertions.assertEquals(records.get(0).getLine(), 0);
        Assertions.assertFalse(records.get(0).getLastSuccess() != 0);
    }

    private void verifyAdding(IndexManagement.IndexFileManager indexFileManager) throws IOException {
        addFile(indexFileManager, spoolDirTest, "2.log");
        addFile(indexFileManager, spoolDirTest, "3.log");
        addFile(indexFileManager, spoolDirTest, "4.log");
        addFile(indexFileManager, spoolDirTest, "5.log");
    }

    private void verifyArchiving(IndexManagement.IndexFileManager indexFileManager) {
        indexFileManager.remove(indexFileManager.getRecords().get(1));
        indexFileManager.remove(indexFileManager.getRecords().get(1));
        indexFileManager.remove(indexFileManager.getRecords().get(1));
        indexFileManager.remove(indexFileManager.getRecords().get(1));

        checkArchiveDir(archiveDir);
    }

    private void verifyRemove(IndexManagement.IndexFileManager indexFileManager) throws IOException {
        indexFileManager.remove(indexFileManager.getRecords().get(5));

        boolean isPending = indexFileManager.getRecords().size() > 0;
        Assertions.assertTrue(isPending);
    }

    private void verifySaveAndLoad(IndexManagement.IndexFileManager indexFileManager) throws IOException {
        indexFileManager.getRecords().get(2).updateFailedAttempt();
        indexFileManager.getRecords().get(3).setDone();
        indexFileManager.getRecords().get(1).setDoneCompleted(333l);
        indexFileManager.getRecords().get(0).setCurrentLine(999);

        Assertions.assertEquals(indexFileManager.getRecords().size(), 6);
    }

    private void checkArchiveDir(File archiveDir) {
        Set<String> availableFiles = new HashSet<>();
        availableFiles.add(new File(archiveDir, "3.log").toString());
        availableFiles.add(new File(archiveDir, "4.log").toString());

        if (!archiveDir.exists()) {
            return;
        }

        File[] files = archiveDir.listFiles();
        Assertions.assertNotNull(files);
        Assertions.assertEquals(files.length, 1);
    }

    private void addFile(IndexManagement.IndexFileManager indexFileManager, String dir, String fileName) throws IOException {
        File file = new File(dir, fileName);
        file.createNewFile();
        indexFileManager.add(file.toString());
    }

    private void checkDoneFile(File newIndexDoneFile, File archiveDir, int maxArchiveFiles, String expectedFilePath) throws IOException {
        IndexManagement.IndexFileManager indexFileManager = new IndexManagement.IndexFileManager(SOURCE_TEST, newIndexDoneFile, newIndexDoneFile, null, maxArchiveFiles);

        Assertions.assertEquals(indexFileManager.getRecords().size(), 2);
        Assertions.assertTrue(indexFileManager.getRecords().get(1).getPath().endsWith(expectedFilePath));
    }

    @AfterAll
    public void tearDown() {
        FileUtils.deleteQuietly(new File(spoolDirTest));
    }
}
