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

package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.UnfilteredDeserializer;
import org.apache.cassandra.io.sstable.format.AbstractSSTableIterator;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;

public abstract class AbstractBigTableIterator extends AbstractSSTableIterator<BigTableRowIndexEntry>
{
    protected AbstractBigTableIterator(SSTableReader sstable,
                                       FileDataInput file,
                                       DecoratedKey key,
                                       BigTableRowIndexEntry indexEntry,
                                       Slices slices,
                                       ColumnFilter columnFilter,
                                       FileHandle ifile)
    {
        super(sstable, file, key, indexEntry, slices, columnFilter, ifile);
    }

    protected abstract class RowReader extends Reader {
        protected UnfilteredDeserializer deserializer;

        // Records the currently open range tombstone (if any)
        protected DeletionTime openMarker;

        protected RowReader(FileDataInput file, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile);

            if (file != null)
                createDeserializer();
        }

        private void createDeserializer()
        {
            assert file != null && deserializer == null;
            deserializer = UnfilteredDeserializer.create(metadata, file, sstable.header, helper);
        }

        public void seekToPosition(long position) throws IOException
        {
            // This may be the first time we're actually looking into the file
            if (file == null)
            {
                file = sstable.getFileDataInput(position);
                createDeserializer();
            }
            else
            {
                file.seek(position);
            }
        }

        protected void updateOpenMarker(RangeTombstoneMarker marker)
        {
            // Note that we always read index blocks in forward order so this method is always called in forward order
            openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : null;
        }
    }
}
