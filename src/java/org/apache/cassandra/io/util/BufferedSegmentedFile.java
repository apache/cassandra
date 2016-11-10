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

public class BufferedSegmentedFile extends SegmentedFile
{
    public BufferedSegmentedFile(ChannelProxy channel, long length)
    {
        super(new Cleanup(channel), channel, length);
    }

    private BufferedSegmentedFile(BufferedSegmentedFile copy)
    {
        super(copy);
    }

    private static class Cleanup extends SegmentedFile.Cleanup
    {
        protected Cleanup(ChannelProxy channel)
        {
            super(channel);
        }
        public void tidy()
        {
            super.tidy();
        }
    }

    public static class Builder extends SegmentedFile.Builder
    {
        public void addPotentialBoundary(long boundary)
        {
            // only one segment in a standard-io file
        }

        public SegmentedFile complete(ChannelProxy channel, long overrideLength)
        {
            long length = overrideLength > 0 ? overrideLength : channel.size();
            return new BufferedSegmentedFile(channel, length);
        }
    }

    public FileDataInput getSegment(long position)
    {
        RandomAccessReader reader = RandomAccessReader.open(channel);
        reader.seek(position);
        return reader;
    }

    public BufferedSegmentedFile sharedCopy()
    {
        return new BufferedSegmentedFile(this);
    }
}
