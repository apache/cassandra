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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import org.apache.cassandra.exceptions.ConfigurationException;

import static java.lang.String.format;

/**
 * Default implementation of {@link AbstractCompressionProvider} 
 * 
 * This provider acts as default implementation which creates compressor for built-in algorithms (LZ4, Snappy, Deflate, etc.)
 * It relies on Cassandra's built-in compression implementations and does not require any external dependencies.
 * It is used when:
 *   No custom compression provider is configured
 *   A custom compression provider fails and fallback is enabled
 */
public class DefaultCompressionProvider extends AbstractCompressionProvider
{
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
     *
     * @param compressionOptions Configuration options for the compressor
     * @return A new ICompressor instance
     * @throws ConfigurationException if the compressor class does not have a valid create method,
     * if there are unknown compression options, or if the compressor creation fails
     */
    @Override
    public ICompressor createCompressor(Class<?> compressorClass, Map<String, String> compressionOptions) throws IllegalStateException
    {
        try
        {
            Method method = compressorClass.getMethod("create", Map.class);
            ICompressor compressor = (ICompressor)method.invoke(null, compressionOptions);
            // Check for unknown options
            for (String provided : compressionOptions.keySet())
                if (!compressor.supportedOptions().contains(provided))
                    throw new ConfigurationException("Unknown compression options " + provided);
            return compressor;
        }
        catch (NoSuchMethodException e)
        {
            throw new ConfigurationException("create method not found", e);
        }
        catch (SecurityException e)
        {
            throw new ConfigurationException("Access forbidden", e);
        }
        catch (IllegalAccessException e)
        {
            throw new ConfigurationException("Cannot access method create in " + compressorClass.getName(), e);
        }
        catch (InvocationTargetException e)
        {
            if (e.getTargetException() instanceof ConfigurationException)
                throw (ConfigurationException) e.getTargetException();

            Throwable cause = e.getCause() == null
                            ? e
                            : e.getCause();

            throw new ConfigurationException(format("%s.create() threw an error: %s %s",
                                                    compressorClass.getSimpleName(),
                                                    cause.getClass().getName(),
                                                    cause.getMessage()),
                                             e);
        }
        catch (ExceptionInInitializerError e)
        {
            throw new ConfigurationException("Cannot initialize class " + compressorClass.getName());
        }
    }
}
