/**
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
 */

package org.apache.cassandra.io.sstable;

import java.util.Collection;
import java.util.Set;

import com.google.common.base.Throwables;

import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * An ISSTableScanner is an abstraction allowing multiple SSTableScanners to be
 * chained together under the hood.  See LeveledCompactionStrategy.getScanners.
 */
public interface ISSTableScanner extends UnfilteredPartitionIterator
{
    public long getLengthInBytes();
    public long getCompressedLengthInBytes();
    public long getCurrentPosition();
    public long getBytesScanned();
    public Set<SSTableReader> getBackingSSTables();

    public static void closeAllAndPropagate(Collection<ISSTableScanner> scanners, Throwable throwable)
    {
        for (ISSTableScanner scanner: scanners)
        {
            try
            {
                scanner.close();
            }
            catch (Throwable t2)
            {
                JVMStabilityInspector.inspectThrowable(t2);
                if (throwable == null)
                {
                    throwable = t2;
                }
                else
                {
                    throwable.addSuppressed(t2);
                }
            }
        }

        if (throwable != null)
        {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }

    }
}
