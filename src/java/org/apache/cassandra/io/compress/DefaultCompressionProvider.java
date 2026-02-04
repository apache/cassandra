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

/**
 * Default implementation of {@link AbstractCompressionProvider} 
 * 
 * This provider acts as a no-op implementation that delegates compressor creation back to the
 * standard Cassandra compression mechanism via {@code CompressionParams}. Unlike plugin providers,
 * this class does not create compressors directly. Instead, it
 * relies on the existing Cassandra compression infrastructure to handle compressor instantiation
 * using the standard built-in algorithms (LZ4, Snappy, Deflate, etc.)
 * It is used when:
 *   No custom compression provider is configured
 *   A custom compression provider fails and fallback is enabled
 */
public class DefaultCompressionProvider extends AbstractCompressionProvider
{
    public DefaultCompressionProvider(Map<String, String> args)
    {
        super(args);
    }

    /**
     * Checks if this compression provider is healthy and ready to use.
     * The default provider is always considered healthy as it has no external
     * dependencies and relies on Cassandra's built-in compression mechanisms.
     *
     * @return Always returns {@code true}
     */
    @Override
    public boolean isHealthy()
    { 
	    return true;
    }

    /**
     * Creates a new compressor instance with the given compression parameters.
     * <p><strong>Note:</strong> This method is not intended to be called for the default provider.
     * Compressor creation is handled by {@code CompressionParams} using the standard Cassandra
     * compression infrastructure. If this method is called, it indicates a programming error
     * in the compression provider selection logic.</p>
     *
     * @param options Configuration options for the compressor (unused)
     * @return Never returns normally
     * @throws IllegalStateException Always thrown, as this method should not be called
     *                               for the default provider
     */
    @Override
    public ICompressor createCompressor(Map<String, String> options) throws IllegalStateException
    {
        throw new IllegalStateException("CompressionParams will create the compressor. It should not get here! ");
    }

    /**
     * Returns the fully qualified class name of this compression provider.
     *
     * @return The complete class name: {@code org.apache.cassandra.io.compress.DefaultCompressionProvider}
     */
    @Override
    public String getProviderName()
    {
        return this.getClass().getName();
    }

    @Override
    /**
     * Returns the simple class name of this compression provider.
     *
     * @return The simple class name: {@code DefaultCompressionProvider}
     */
    public String getProviderSimpleName()
    {
        return this.getClass().getSimpleName();
    }

    /**
     * Returns the name of the compressor supported by this provider.
     * The default provider returns an empty string because it doesn't support
     * a specific compressor type. Instead, it delegates to the standard Cassandra
     * compression mechanism which supports all built-in compressor types
     * (LZ4Compressor, ZstdCompressor, DeflateCompressor, etc.).
     *
     * @return Empty string, indicating this provider doesn't target a specific compressor type
     */
    @Override
    public String getSupportedCompressorName()
 {
	 return "";
 }
}
