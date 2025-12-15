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

package org.apache.cassandra.io.compress;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.exceptions.ConfigurationException;

import static java.lang.String.format;

/**
 * Interface for compressors that support dictionary-based compression.
 * <br>
 * Dictionary compressors can use pre-trained compression dictionaries to achieve
 * better compression ratios, especially for small data chunks that are similar
 * to the training data used to create the dictionary.
 *
 * @param <T> the specific type of compression dictionary this compressor supports
 */
public interface IDictionaryCompressor<T extends CompressionDictionary>
{
    String TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME = "training_max_dictionary_size";
    String DEFAULT_TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_VALUE = "64KiB";

    String TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME = "training_max_total_sample_size";
    String DEFAULT_TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_VALUE = "10MiB";

    /**
     * Validates value of a parameter for training purposes. The value to validate should
     * be accepted by {@link DataStorageSpec.IntKibibytesBound}. This method is used upon validation
     * of input parameters in the implementations of dictionary compressor.
     *
     * @param parameterName name of a parameter to validate
     * @param resolvedValue value to validate
     */
    static void validateTrainingParameter(String parameterName, String resolvedValue)
    {
        try
        {
            new DataStorageSpec.IntKibibytesBound(resolvedValue).toBytes();
        }
        catch (Throwable t)
        {
            throw new ConfigurationException(format("Unable to set value to parameter %s: %s. Reason: %s",
                                                    parameterName, resolvedValue, t.getMessage()));
        }
    }

    /**
     * Returns a compressor instance configured with the specified compression dictionary.
     * <br>
     * This method may return the same instance if it already uses the given dictionary,
     * or create a new instance configured with the dictionary. The implementation should
     * be efficient and avoid unnecessary object creation when possible.
     *
     * @param compressionDictionary the dictionary to use for compression/decompression
     * @return a compressor instance that will use the specified dictionary
     */
    ICompressor getOrCopyWithDictionary(T compressionDictionary);

    /**
     * Returns the kind of compression dictionary that this compressor can accept.
     * <br>
     * This is used to validate dictionary compatibility before attempting to use
     * a dictionary with this compressor. Only dictionaries of the returned kind
     * should be passed to {@link #getOrCopyWithDictionary(CompressionDictionary)}.
     *
     * @return the compression dictionary kind supported by this compressor
     */
    CompressionDictionary.Kind acceptableDictionaryKind();

    /**
     * Checks whether this compressor can use the given compression dictionary.
     * <br>
     * The default implementation compares the dictionary's kind with the kind
     * returned by {@link #acceptableDictionaryKind()}. Compressor implementations
     * may override this method to provide more sophisticated compatibility checks.
     *
     * @param dictionary the compression dictionary to check for compatibility
     * @return true if this compressor can use the dictionary, false otherwise
     */
    default boolean canConsumeDictionary(CompressionDictionary dictionary)
    {
        return dictionary.kind() == acceptableDictionaryKind();
    }
}
