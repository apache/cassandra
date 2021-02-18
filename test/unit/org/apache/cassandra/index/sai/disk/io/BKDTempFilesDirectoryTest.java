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
package org.apache.cassandra.index.sai.disk.io;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.bkd.BKDReader;
import org.apache.lucene.util.bkd.BKDWriter;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class BKDTempFilesDirectoryTest extends NdiRandomizedTest
{
    @Test
    public void shouldSortPointsOnDisk() throws IOException
    {
        final int numRows = between(300_000, 500_000);
        final IndexComponents indexComponents = newIndexComponents();
        final TempFileTrackingDirectoryWrapper directoryWrapper = 
                new TempFileTrackingDirectoryWrapper(new BKDTempFilesDirectory(indexComponents, randomLong()));

        try (final BKDWriter w = new BKDWriter(numRows,
                                               directoryWrapper,
                                               "tmp",
                                               1,
                                               4,
                                               BKDWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE,
                                               // low threshold
                                               1.0,
                                               numRows,
                                               true))
        {

            byte[] scratch = new byte[4];
            for (int segmentRowId = 0; segmentRowId < numRows; ++segmentRowId)
            {
                NumericUtils.intToSortableBytes(segmentRowId, scratch, 0);
                w.add(scratch, segmentRowId);
            }

            long indexFP;
            
            try (IndexOutput out = indexComponents.createOutput(indexComponents.kdTree))
            {
                indexFP = w.finish(out);
            }
            
            assertThat(directoryWrapper.createdTempFiles.size(), is(greaterThan(0)));
    
            try (final IndexInput indexInput = indexComponents.openBlockingInput(indexComponents.kdTree))
            {
                indexInput.seek(indexFP);
                final BKDReader bkdReader = new BKDReader(indexInput);
                assertEquals(numRows, bkdReader.getDocCount());
            }
        }
    }

    private static class TempFileTrackingDirectoryWrapper extends FilterDirectory
    {
        private final Set<String> createdTempFiles = new HashSet<>();

        TempFileTrackingDirectoryWrapper(Directory in)
        {
            super(in);
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException
        {
            IndexOutput output = super.createTempOutput(prefix, suffix, context);
            createdTempFiles.add(output.getName());
            return output;
        }

        @Override
        public void deleteFile(String name) throws IOException
        {
            assertTrue(createdTempFiles.contains(name));
            super.deleteFile(name);
        }
    }
}
