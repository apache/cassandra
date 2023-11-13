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

import java.io.IOException;

import org.apache.cassandra.utils.ChecksumType;

public final class ChecksummedRandomAccessReader
{
    @SuppressWarnings({ "resource", "RedundantSuppression" }) // The Rebufferer owns both the channel and the validator and handles closing both.
    public static RandomAccessReader open(File file, File crcFile) throws IOException
    {
        ChannelProxy channel = new ChannelProxy(file);
        try
        {
            DataIntegrityMetadata.ChecksumValidator validator = new DataIntegrityMetadata.ChecksumValidator(ChecksumType.CRC32,
                                                                                                            RandomAccessReader.open(crcFile),
                                                                                                            file.path());
            Rebufferer rebufferer = new ChecksummedRebufferer(channel, validator);
            return new RandomAccessReader.RandomAccessReaderWithOwnChannel(rebufferer);
        }
        catch (Throwable t)
        {
            channel.close();
            throw t;
        }
    }
}
