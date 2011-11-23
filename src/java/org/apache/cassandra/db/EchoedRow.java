package org.apache.cassandra.db;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.DataOutput;
import java.io.IOException;
import java.security.MessageDigest;

import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;

/**
 * A CompactedRow implementation that just echos the original row bytes without deserializing.
 * Currently only used by cleanup.
 */
public class EchoedRow extends AbstractCompactedRow
{
    private final SSTableIdentityIterator row;

    public EchoedRow(SSTableIdentityIterator row)
    {
        super(row.getKey());
        this.row = row;
        // Reset SSTableIdentityIterator because we have not guarantee the filePointer hasn't moved since the Iterator was built
        row.reset();
    }

    public long write(DataOutput out) throws IOException
    {
        assert row.dataSize > 0;
        out.writeLong(row.dataSize);
        row.echoData(out);
        return row.dataSize;
    }

    public void update(MessageDigest digest)
    {
        // EchoedRow is not used in anti-entropy validation
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty()
    {
        // never okay to purge a EchoedRow -- if it were, we'd need to deserialize instead of echoing
        return false;
    }

    public int columnCount()
    {
        return row.getColumnCount();
    }

    public long maxTimestamp()
    {
        return Long.MIN_VALUE;
    }
}
