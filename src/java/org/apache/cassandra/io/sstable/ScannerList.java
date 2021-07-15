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

import java.util.List;

import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

public class ScannerList implements AutoCloseable
{
    public final List<ISSTableScanner> scanners;
    public ScannerList(List<ISSTableScanner> scanners)
    {
        this.scanners = scanners;
    }

    public long getTotalBytesScanned()
    {
        long bytesScanned = 0L;
        for (ISSTableScanner scanner : scanners)
            bytesScanned += scanner.getBytesScanned();

        return bytesScanned;
    }

    public long getTotalCompressedSize()
    {
        long compressedSize = 0;
        for (int i=0, isize=scanners.size(); i<isize; i++)
            compressedSize += scanners.get(i).getCompressedLengthInBytes();

        return compressedSize;
    }

    public double getCompressionRatio()
    {
        double compressed = 0.0;
        double uncompressed = 0.0;

        for (int i=0, isize=scanners.size(); i<isize; i++)
        {
            @SuppressWarnings("resource")
            ISSTableScanner scanner = scanners.get(i);
            compressed += scanner.getCompressedLengthInBytes();
            uncompressed += scanner.getLengthInBytes();
        }

        if (compressed == uncompressed || uncompressed == 0)
            return MetadataCollector.NO_COMPRESSION_RATIO;

        return compressed / uncompressed;
    }

    public void close()
    {
        ISSTableScanner.closeAllAndPropagate(scanners, null);
    }
}