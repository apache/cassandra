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
package org.apache.cassandra.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.LeveledManifest;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMetadata;
import org.apache.cassandra.utils.Pair;

/**
 * Reset level to 0 on a given set of sstables
 */
public class SSTableLevelResetter
{
    /**
     * @param args a list of sstables whose metadata we are changing
     */
    public static void main(String[] args) throws IOException
    {
        PrintStream out = System.out;
        if (args.length == 0)
        {
            out.println("This command should be run with Cassandra stopped!");
            out.println("Usage: sstablelevelreset <keyspace> <columnfamily>");
            System.exit(1);
        }

        if (!args[0].equals("--really-reset") || args.length != 3)
        {
            out.println("This command should be run with Cassandra stopped, otherwise you will get very strange behavior");
            out.println("Verify that Cassandra is not running and then execute the command like this:");
            out.println("Usage: sstablelevelreset --really-reset <keyspace> <columnfamily>");
            System.exit(1);
        }

        String keyspace = args[1];
        String columnfamily = args[2];
        Directories directories = Directories.create(keyspace, columnfamily);
        boolean foundSSTable = false;
        for (Map.Entry<Descriptor, Set<Component>> sstable : directories.sstableLister().list().entrySet())
        {
            if (sstable.getValue().contains(Component.STATS))
            {
                foundSSTable = true;
                Descriptor descriptor = sstable.getKey();
                Pair<SSTableMetadata, Set<Integer>> metadata = SSTableMetadata.serializer.deserialize(descriptor);
                out.println("Changing level from " + metadata.left.sstableLevel + " to 0 on " + descriptor.filenameFor(Component.DATA));
                LeveledManifest.mutateLevel(metadata, descriptor, descriptor.filenameFor(Component.STATS), 0);
            }
        }

        if (!foundSSTable)
        {
            out.println("Found no sstables, did you give the correct keyspace/columnfamily?");
        }
    }
}
