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

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Set;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.util.File;

public class CompressionInfoComponent
{
    public static CompressionMetadata maybeLoad(Descriptor descriptor, Set<Component> components)
    {
        if (components.contains(Components.COMPRESSION_INFO))
            return load(descriptor);

        return null;
    }

    public static CompressionMetadata loadIfExists(Descriptor descriptor)
    {
        if (descriptor.fileFor(Components.COMPRESSION_INFO).exists())
            return load(descriptor);

        return null;
    }

    public static CompressionMetadata load(Descriptor descriptor)
    {
        return CompressionMetadata.open(descriptor.fileFor(Components.COMPRESSION_INFO),
                                        descriptor.fileFor(Components.DATA).length(),
                                        descriptor.version.hasMaxCompressedLength());
    }

    /**
     * Best-effort checking to verify the expected compression info component exists, according to the TOC file.
     * The verification depends on the existence of TOC file. If absent, the verification is skipped.
     *
     * @param descriptor
     * @param actualComponents actual components listed from the file system.
     * @throws CorruptSSTableException if TOC expects compression info but not found from disk.
     * @throws FSReadError             if unable to read from TOC file.
     */
    public static void verifyCompressionInfoExistenceIfApplicable(Descriptor descriptor, Set<Component> actualComponents) throws CorruptSSTableException, FSReadError
    {
        File tocFile = descriptor.fileFor(Components.TOC);
        if (tocFile.exists())
        {
            try
            {
                Set<Component> expectedComponents = TOCComponent.loadTOC(descriptor, false);
                if (expectedComponents.contains(Components.COMPRESSION_INFO) && !actualComponents.contains(Components.COMPRESSION_INFO))
                {
                    File compressionInfoFile = descriptor.fileFor(Components.COMPRESSION_INFO);
                    throw new CorruptSSTableException(new NoSuchFileException(compressionInfoFile.absolutePath()), compressionInfoFile);
                }
            }
            catch (IOException e)
            {
                throw new FSReadError(e, tocFile);
            }
        }
    }
}
