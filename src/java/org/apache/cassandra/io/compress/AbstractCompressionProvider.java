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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for compression providers.
 * Provides common functionality for loading and managing compressors
 * with configurable fallback behavior for plugin compression libraries.
 */
public abstract class AbstractCompressionProvider
{
    protected static final Logger logger = LoggerFactory.getLogger(AbstractCompressionProvider.class);

    public AbstractCompressionProvider()
    {
    }

    /**
     * Returns the fully qualified class name of the compression provider.
     *
     * @return Fully qualified name of the provider
     */
    public abstract String getProviderName();

    /**
     * Returns the simple class name of the compression provider.
     * 
     * @return Class name of the provider
     */
    public abstract String getProviderSimpleName();
   
    /**
     * Checks if this compression provider is in a healthy state and ready to use.
     *
     * <p>This method should perform any necessary health checks, such as verifying
     * that required native libraries are loaded, able to do compress/decompress successfully, etc.</p>
     *
     * @return true if the provider is healthy and ready to use, false otherwise
     * @throws Exception if an error occurs during the health check
     */
    public abstract boolean isHealthy() throws Exception;

    /**
     * Creates a new compressor instance with the given compression parameters.
     * 
     * <p>This method is called when a new compressor is needed. The implementation
     * should create and configure a compressor based on the provided options.</p>
     * 
     * @param options Configuration options for the compressor. May be null or empty.
     * @return A new ICompressor instance
     * @throws IllegalStateException if the compressor cannot be created
     */
    public abstract ICompressor createCompressor(Class<?> compressorClass, Map<String, String> options) throws IllegalStateException;

}
