/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.lifecycle;

import java.io.File;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.util.FileUtils;
import org.mockito.Mockito;

public class LogReplicationSetTest
{
    @Test
    public void shouldThrowIfAppendFailedToAllReplicas() throws Throwable
    {
        int nrReplicas = 2;
        LogReplicaSet replicas = new LogReplicaSet();
        ArrayList<File> spyFiles = getSpyFiles("testAppendFailedToAll", nrReplicas);

        replicas.addReplicas(spyFiles);
        spyFiles.forEach(f -> Mockito.when(f.exists()).thenThrow(new RuntimeException()));

        Assert.assertThrows(RuntimeException.class,
                            () ->
                            replicas.append(LogRecord.makeAbort(System.currentTimeMillis())));
    }

    @Test
    public void shouldNotThrowIfAppendFailedToSomeReplicas() throws Throwable
    {
        int nrReplicas = 2;
        LogReplicaSet replicas = new LogReplicaSet();
        ArrayList<File> spyFiles = getSpyFiles("testAppendFailedToSome", nrReplicas);

        replicas.addReplicas(spyFiles);
        Mockito.when(spyFiles.get(0).exists()).thenThrow(new RuntimeException());
    }

    private ArrayList<File> getSpyFiles(String testName, int nrReplicas)
    {
        ArrayList<File> files = new ArrayList<>(nrReplicas);
        for (int i = 0; i < nrReplicas; i++)
        {
            files.add(Mockito.spy(createTempFile(testName, i)));
        }
        return files;
    }

    private static File createTempFile(String testName, int id)
    {
        String prefix = String.format("%s_%d", testName, id);
        File dir = new File(FileUtils.getTempDir(), prefix);

        FileUtils.createDirectory(dir);
        File file = FileUtils.createTempFile(prefix, "tmp", dir);

        file.deleteOnExit();
        dir.deleteOnExit();
        return file;
    }
}
