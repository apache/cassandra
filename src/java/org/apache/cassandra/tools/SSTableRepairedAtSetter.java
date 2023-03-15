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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.util.File;

public class SSTableRepairedAtSetter
{
    /**
     * @param args a list of sstables whose metadata we are changing
     */
    public static void main(final String[] args) throws IOException
    {
        PrintStream out = System.out;
        if (args.length == 0)
        {
            out.println("This command should be run with Cassandra stopped!");
            out.println("Usage: sstablerepairedset [--is-repaired | --is-unrepaired] [-f <sstable-list> | <sstables>]");
            System.exit(1);
        }

        if (args.length < 3 || !args[0].equals("--really-set") || (!args[1].equals("--is-repaired") && !args[1].equals("--is-unrepaired")))
        {
            out.println("This command should be run with Cassandra stopped, otherwise you will get very strange behavior");
            out.println("Verify that Cassandra is not running and then execute the command like this:");
            out.println("Usage: sstablerepairedset --really-set [--is-repaired | --is-unrepaired] [-f <sstable-list> | <sstables>]");
            System.exit(1);
        }

        Util.initDatabaseDescriptor();

        boolean setIsRepaired = args[1].equals("--is-repaired");

        List<String> fileNames;
        if (args[2].equals("-f"))
        {
            fileNames = Files.readAllLines(File.getPath(args[3]), Charset.defaultCharset());
        }
        else
        {
            fileNames = Arrays.asList(args).subList(2, args.length);
        }

        for (String fname: fileNames)
        {
            Descriptor descriptor = Descriptor.fromFileWithComponent(new File(fname), false).left;
            if (!descriptor.version.isCompatible())
            {
                System.err.println("SSTable " + fname + " is in a old and unsupported format");
                continue;
            }

            if (setIsRepaired)
            {
                FileTime f = Files.getLastModifiedTime(descriptor.fileFor(Components.DATA).toPath());
                descriptor.getMetadataSerializer().mutateRepairMetadata(descriptor, f.toMillis(), null, false);
            }
            else
            {
                descriptor.getMetadataSerializer().mutateRepairMetadata(descriptor, 0, null, false);
            }
        }
    }
}
