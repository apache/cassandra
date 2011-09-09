package org.apache.cassandra.io.util;
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


import java.io.File;
import java.io.IOError;
import java.io.IOException;

public class BufferedSegmentedFile extends SegmentedFile
{
    public BufferedSegmentedFile(String path, long length)
    {
        super(path, length);
    }

    public static class Builder extends SegmentedFile.Builder
    {
        /**
         * Adds a position that would be a safe place for a segment boundary in the file. For a block/row based file
         * format, safe boundaries are block/row edges.
         * @param boundary The absolute position of the potential boundary in the file.
         */
        public void addPotentialBoundary(long boundary)
        {
            // only one segment in a standard-io file
        }

        /**
         * Called after all potential boundaries have been added to apply this Builder to a concrete file on disk.
         * @param path The file on disk.
         */
        public SegmentedFile complete(String path)
        {
            long length = new File(path).length();
            return new BufferedSegmentedFile(path, length);
        }
    }

    public FileDataInput getSegment(long position)
    {
        try
        {
            RandomAccessReader file = RandomAccessReader.open(new File(path));
            file.seek(position);
            return file;
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public void cleanup()
    {
        // nothing to do
    }
}
