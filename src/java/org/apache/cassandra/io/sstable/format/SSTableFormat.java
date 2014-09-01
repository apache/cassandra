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
package org.apache.cassandra.io.sstable.format;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.LegacyLayout;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.FileDataInput;

import java.util.Iterator;

/**
 * Provides the accessors to data on disk.
 */
public interface SSTableFormat
{
    static boolean enableSSTableDevelopmentTestMode = Boolean.valueOf(System.getProperty("cassandra.test.sstableformatdevelopment","false"));


    Version getLatestVersion();
    Version getVersion(String version);

    SSTableWriter.Factory getWriterFactory();
    SSTableReader.Factory getReaderFactory();

    RowIndexEntry.IndexSerializer<?> getIndexSerializer(CFMetaData cfm, Version version, SerializationHeader header);

    public static enum Type
    {
        //Used internally to refer to files with no
        //format flag in the filename
        LEGACY("big", BigFormat.instance),

        //The original sstable format
        BIG("big", BigFormat.instance);

        public final SSTableFormat info;
        public final String name;

        private Type(String name, SSTableFormat info)
        {
            //Since format comes right after generation
            //we disallow formats with numeric names
            assert !CharMatcher.DIGIT.matchesAllOf(name);

            this.name = name;
            this.info = info;
        }

        public static Type validate(String name)
        {
            for (Type valid : Type.values())
            {
                //This is used internally for old sstables
                if (valid == LEGACY)
                    continue;

                if (valid.name.equalsIgnoreCase(name))
                    return valid;
            }

            throw new IllegalArgumentException("No Type constant " + name);
        }
    }
}
