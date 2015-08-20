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

package org.apache.cassandra.io.sstable;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Transactional;

public interface SSTableMultiWriter extends Transactional
{

    /**
     * Writes a partition in an implementation specific way
     * @param partition the partition to append
     * @return true if the partition was written, false otherwise
     */
    boolean append(UnfilteredRowIterator partition);

    Collection<SSTableReader> finish(long repairedAt, long maxDataAge, boolean openResult);
    Collection<SSTableReader> finish(boolean openResult);
    Collection<SSTableReader> finished();

    SSTableMultiWriter setOpenResult(boolean openResult);

    String getFilename();
    long getFilePointer();
    UUID getCfId();

    static void abortOrDie(SSTableMultiWriter writer)
    {
        Throwables.maybeFail(writer.abort(null));
    }
}
