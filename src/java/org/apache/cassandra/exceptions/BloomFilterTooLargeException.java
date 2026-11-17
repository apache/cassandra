/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.exceptions;

import org.apache.cassandra.utils.FBUtilities;

/**
 * Thrown when a bloom filter would need more than 16 GiB of off-heap memory, see {@code OffHeapBitSet}.
 * <p>
 * The failure is deterministic: it depends only on the number of keys and on {@code bloom_filter_fp_chance}, so
 * repeating the operation on the same sstables fails in the same way.
 * <p>
 * It extends {@link UnsupportedOperationException}, which {@code OffHeapBitSet} threw before, and must not be turned
 * into a {@code MemoryLimiter.ReachedMemoryLimitException}: {@code FilterFactory} would then fall back to an
 * always-present filter, which would make every read hit the index of an sstable with billions of keys.
 */
public class BloomFilterTooLargeException extends UnsupportedOperationException
{
    public BloomFilterTooLargeException(long requiredBytes)
    {
        super(String.format("Bloom filter would need %s, which exceeds the 16 GiB limit; increase bloom_filter_fp_chance",
                            FBUtilities.prettyPrintMemory(requiredBytes, " ")));
    }
}
