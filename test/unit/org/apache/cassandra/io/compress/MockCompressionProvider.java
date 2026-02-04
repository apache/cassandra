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

public class MockCompressionProvider extends AbstractCompressionProvider
{
    public MockCompressionProvider(Map<String, String> args)
    {
        super(args);
    }

    @Override
    public boolean isHealthy()
    { return false;}

    @Override
    public ICompressor createCompressor(Map<String, String> options) throws IllegalStateException
    {
        throw new IllegalStateException("This is a mock compressor for testing! ");
    }

    @Override
    public String getProviderName()
    {
        return this.getClass().getName();
    }

    @Override
    public String getProviderSimpleName()
    {
        return this.getClass().getSimpleName();
    }

    @Override
    public String getSupportedCompressorName()
    {
        return "";
    }
}

