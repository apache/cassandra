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
package org.apache.cassandra.io.util;

import java.io.File;

import org.apache.cassandra.io.sstable.format.SSTableWriter;

public class BufferedSegmentedFile extends SegmentedFile
{
    public BufferedSegmentedFile(String path, long length)
    {
        super(new Cleanup(path), path, length);
    }

    private BufferedSegmentedFile(BufferedSegmentedFile copy)
    {
        super(copy);
    }

    private static class Cleanup extends SegmentedFile.Cleanup
    {
        protected Cleanup(String path)
        {
            super(path);
        }
        public void tidy() throws Exception
        {

        }
    }

    public static class Builder extends SegmentedFile.Builder
    {
        public void addPotentialBoundary(long boundary)
        {
            // only one segment in a standard-io file
        }

        public SegmentedFile complete(String path, long overrideLength, boolean isFinal)
        {
            assert !isFinal || overrideLength <= 0;
            long length = overrideLength > 0 ? overrideLength : new File(path).length();
            return new BufferedSegmentedFile(path, length);
        }
    }

    public FileDataInput getSegment(long position)
    {
        RandomAccessReader reader = RandomAccessReader.open(new File(path));
        reader.seek(position);
        return reader;
    }

    public BufferedSegmentedFile sharedCopy()
    {
        return new BufferedSegmentedFile(this);
    }
}
