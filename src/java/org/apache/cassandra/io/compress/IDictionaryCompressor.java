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

import org.apache.cassandra.db.compression.CompressionDictionary;

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

    String TRAINING_MIN_FREQUENCY_PARAMETER_NAME = "training_min_frequency";
    // 0m means there is no limit how often we can train, if this is set to e.g. 1h, that means
    // that once we train a dictionary for given table, then we can train again after at least 1 hour.
    String DEFAULT_TRAINING_MIN_FREQUENCY = "0m";

    String AUTO_TRAINING_ENABLED = "auto_training_enabled";

    /**
     * For tables using {@code TimeWindowCompactionStrategy}, the maximum number of TWCS time windows, counted back
     * from the newest, that auto-training may sample from. The resolver walks windows newest-first and stops as soon
     * as it has gathered enough sample data to train on, but never reaches back more than this many windows. The
     * default of 1 restricts sampling to the newest window only (strongest recency bias); raise it to allow training
     * to draw on older windows when the newest one does not hold enough data on its own.
     */
    String AUTO_TRAINING_TWCS_MAX_WINDOWS = "auto_training_twcs_max_windows";
    String DEFAULT_AUTO_TRAINING_TWCS_MAX_WINDOWS_VALUE = "1";

    /**
     * Minimum relative improvement in the compression ratio required to adopt a freshly trained
     * dictionary over the current one (e.g., 0.15 means the candidate must compress the training
     * sample at least 15% better). Set to 0 to adopt on any improvement.
     */
    String AUTO_TRAINING_IMPROVEMENT_THRESHOLD_NAME = "auto_training_improvement_threshold";
    String DEFAULT_AUTO_TRAINING_IMPROVEMENT_THRESHOLD_VALUE = "0.15";

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
