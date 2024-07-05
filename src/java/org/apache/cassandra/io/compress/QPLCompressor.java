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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.intel.qpl.QPLException;
import com.intel.qpl.QPLUtils;

import io.netty.util.concurrent.FastThreadLocal;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class QPLCompressor implements ICompressor
{
    private static final Logger logger = LoggerFactory.getLogger(QPLCompressor.class);
    private static final Set<String> VALID_EXECUTION_PATH = new HashSet<>(Arrays.asList("auto", "hardware", "software"));
    private static final Map<String, QPLUtils.ExecutionPaths> executionPathMap = createMap();

    public static final String DEFAULT_EXECUTION_PATH = "hardware";
    public static final int DEFAULT_COMPRESSION_LEVEL = 1;
    public static final int DEFAULT_RETRY_COUNT = 0;

    //If hardware path is not available then redirect it to software path
    public static final String REDIRECT_EXECUTION_PATH = "software";

    //Configurable parameters name
    public static final String QPL_EXECUTION_PATH = "execution_path";
    public static final String QPL_COMPRESSOR_LEVEL = "compressor_level";
    public static final String QPL_RETRY_COUNT = "retry_count";

    private final Set<Uses> recommendedUses;

    public int initialCompressedBufferLength(int chunkLength)
    {
        if (chunkLength <= 0)
        {
            return 0;
        }
        return com.intel.qpl.QPLCompressor.maxCompressedLength(chunkLength);
    }

    public static QPLCompressor create(Map<String, String> options)
    {
        return new QPLCompressor(options);
    }

    @VisibleForTesting
    String executionPath;
    @VisibleForTesting
    Integer compressionLevel;
    @VisibleForTesting
    Integer retryCount;
    private final FastThreadLocal<com.intel.qpl.QPLCompressor> reusableCompressor;

    private QPLCompressor(Map<String, String> options)
    {
        this.executionPath = validateExecutionPath(options.get(QPL_EXECUTION_PATH));
        this.compressionLevel = validateCompressionLevel(options.get(QPL_COMPRESSOR_LEVEL), this.executionPath);
        this.retryCount = validateRetryCount(options.get(QPL_RETRY_COUNT));
        reusableCompressor = new FastThreadLocal<>()
        {
            @Override
            protected com.intel.qpl.QPLCompressor initialValue()
            {
                return new com.intel.qpl.QPLCompressor(executionPathMap.get(executionPath), compressionLevel, retryCount);
            }
        };

        recommendedUses = ImmutableSet.of(Uses.GENERAL);
        logger.trace("Creating QPLCompressor with execution path {}  and compression level {} add retry_count {}.", this.executionPath, this.compressionLevel, this.retryCount);
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        try
        {
            if (inputLength > 0)
            {
                return reusableCompressor.get().decompress(input, inputOffset, inputLength, output, outputOffset, output.length - outputOffset);
            }
        }
        catch (IllegalArgumentException | ArrayIndexOutOfBoundsException |
               IllegalStateException | QPLException e)
        {
            throw new IOException(e);
        }
        return 0;
    }

    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        try
        {
            if (input.hasRemaining())
            {
                reusableCompressor.get().compress(input, output);
            }
        }
        catch (IllegalArgumentException | ArrayIndexOutOfBoundsException |
               IllegalStateException | QPLException e)
        {
            throw new IOException(e);
        }
    }

    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        try
        {
            if (input.hasRemaining())
            {
                reusableCompressor.get().decompress(input, output);
            }
        }
        catch (IllegalArgumentException | ArrayIndexOutOfBoundsException |
               IllegalStateException | QPLException e)
        {
            throw new IOException(e);
        }
    }

    public BufferType preferredBufferType()
    {
        return BufferType.OFF_HEAP;
    }

    public boolean supports(BufferType bufferType)
    {
        return bufferType == BufferType.OFF_HEAP;
    }

    public Set<String> supportedOptions()
    {
        return new HashSet<>(Arrays.asList(QPL_EXECUTION_PATH, QPL_COMPRESSOR_LEVEL, QPL_RETRY_COUNT));
    }

    @Override
    public Set<Uses> recommendedUses()
    {
        return recommendedUses;
    }

    public static String validateExecutionPath(String executionPath) throws ConfigurationException
    {
        String validExecPath;
        if (executionPath == null)
        {
            //if hardware path is not available then set it to software path.
            executionPath = DEFAULT_EXECUTION_PATH;
            validExecPath = com.intel.qpl.QPLCompressor.getValidExecutionPath(executionPathMap.get(executionPath)).name();
            if (!QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE.name().equalsIgnoreCase(validExecPath))
            {
                logger.warn("The execution path 'hardware' is not available hence default it to software.");
                executionPath = REDIRECT_EXECUTION_PATH;
            }
            return executionPath;
        }


        if (!VALID_EXECUTION_PATH.contains(executionPath))
        {
            throw new ConfigurationException(String.format("Invalid execution path '%s' specified for QPLCompressor parameter '%s'. "
                                                           + "Valid options are %s.", executionPath, QPL_EXECUTION_PATH,
                                                           VALID_EXECUTION_PATH));
        }
        else
        {
            validExecPath = com.intel.qpl.QPLCompressor.getValidExecutionPath(executionPathMap.get(executionPath)).name();
            if (!validExecPath.equalsIgnoreCase(executionPathMap.get(executionPath).name()))
            {
                logger.warn("The execution path '{}' is not available hence default it to software.", executionPath);
                executionPath = REDIRECT_EXECUTION_PATH;
            }
            return executionPath;
        }
    }

    public static int validateCompressionLevel(String compressionLevel, String execPath) throws ConfigurationException
    {
        if (compressionLevel == null)
            return DEFAULT_COMPRESSION_LEVEL;

        ConfigurationException ex = new ConfigurationException("Invalid value [" + compressionLevel + "] for parameter '"
                                                               + QPL_COMPRESSOR_LEVEL + "'.");

        int level;
        try
        {
            level = Integer.parseInt(compressionLevel);
            if (level < 0)
                throw ex;
        }
        catch (NumberFormatException e)
        {
            throw ex;
        }

        int validLevel = com.intel.qpl.QPLCompressor.getValidCompressionLevel(executionPathMap.get(execPath), level);
        if (level != validLevel)
        {
            logger.warn("The compression level '{}' is not supported for {} execution path hence its default to {}.", level, execPath, validLevel);
        }
        return validLevel;
    }

    public static int validateRetryCount(String retryCount) throws ConfigurationException
    {
        if (retryCount == null)
            return DEFAULT_RETRY_COUNT;

        ConfigurationException ex = new ConfigurationException("Invalid value [" + retryCount + "] for parameter '"
                                                               + QPL_RETRY_COUNT + "'. Value must be >= 0.");

        int count;
        try
        {
            count = Integer.parseInt(retryCount);
            if (count < 0)
            {
                throw ex;
            }
        }
        catch (NumberFormatException e)
        {
            throw ex;
        }

        return count;
    }

    private static Map<String, QPLUtils.ExecutionPaths> createMap()
    {
        return ImmutableMap.of("auto", QPLUtils.ExecutionPaths.QPL_PATH_AUTO, "hardware", QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, "software", QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE);
    }
}
