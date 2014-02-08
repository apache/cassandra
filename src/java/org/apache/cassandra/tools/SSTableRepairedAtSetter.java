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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.service.ActiveRepairService;

/**
 * Set repairedAt status on a given set of sstables.
 *
 * If you pass --is-repaired, it will set the repairedAt time to the last modified time.
 *
 * If you know you ran repair 2 weeks ago, you can do something like
 *
 * for x in $(find /var/lib/cassandra/data/.../ -iname "*Data.db*" -mtime +14); do sstablerepairset --is-repaired $x; done
 *
 */
public class SSTableRepairedAtSetter
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
            out.println("Usage: sstablerepairedset [--is-repaired | --is-unrepaired] <sstable>");
            System.exit(1);
        }

        for (String s : args)
            System.out.println(s);

        if (args.length != 3 || !args[0].equals("--really-set") || (!args[1].equals("--is-repaired") && !args[1].equals("--is-unrepaired")))
        {
            out.println("This command should be run with Cassandra stopped, otherwise you will get very strange behavior");
            out.println("Verify that Cassandra is not running and then execute the command like this:");
            out.println("Usage: sstablelevelreset --really-set [--is-repaired | --is-unrepaired] <sstable>");
            System.exit(1);
        }

        boolean setIsRepaired = args[1].equals("--is-repaired");

        String fname = args[2];
        Descriptor descriptor = Descriptor.fromFilename(fname);
        if (setIsRepaired)
        {
            FileTime f = Files.getLastModifiedTime(new File(descriptor.filenameFor(Component.DATA)).toPath());
            descriptor.getMetadataSerializer().mutateRepairedAt(descriptor, f.toMillis());
        }
        else
        {
            descriptor.getMetadataSerializer().mutateRepairedAt(descriptor, ActiveRepairService.UNREPAIRED_SSTABLE);
        }

    }
}
