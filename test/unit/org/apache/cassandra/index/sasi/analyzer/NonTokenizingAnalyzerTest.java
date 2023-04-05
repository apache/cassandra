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
package org.apache.cassandra.index.sasi.analyzer;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the non-tokenizing analyzer
 */
public class NonTokenizingAnalyzerTest
{
    @Test
    public void caseInsensitiveAnalizer() throws Exception
    {
        NonTokenizingAnalyzer analyzer = new NonTokenizingAnalyzer();
        NonTokenizingOptions options = NonTokenizingOptions.getDefaultOptions();
        options.setCaseSensitive(false);
        analyzer.init(options, UTF8Type.instance);

        String testString = "Nip it in the bud";
        ByteBuffer toAnalyze = ByteBuffer.wrap(testString.getBytes());
        analyzer.reset(toAnalyze);
        ByteBuffer analyzed = null;
        while (analyzer.hasNext())
            analyzed = analyzer.next();
        Assert.assertTrue(testString.toLowerCase().equals(ByteBufferUtil.string(analyzed)));
    }

    @Test
    public void caseSensitiveAnalizer() throws Exception
    {
        NonTokenizingAnalyzer analyzer = new NonTokenizingAnalyzer();
        NonTokenizingOptions options = NonTokenizingOptions.getDefaultOptions();
        analyzer.init(options, UTF8Type.instance);

        String testString = "Nip it in the bud";
        ByteBuffer toAnalyze = ByteBuffer.wrap(testString.getBytes());
        analyzer.reset(toAnalyze);
        ByteBuffer analyzed = null;
        while (analyzer.hasNext())
            analyzed = analyzer.next();
        Assert.assertFalse(testString.toLowerCase().equals(ByteBufferUtil.string(analyzed)));
    }

    @Test
    public void ensureIncompatibleInputSkipped() throws Exception
    {
        NonTokenizingAnalyzer analyzer = new NonTokenizingAnalyzer();
        NonTokenizingOptions options = NonTokenizingOptions.getDefaultOptions();
        analyzer.init(options, Int32Type.instance);

        ByteBuffer toAnalyze = ByteBufferUtil.bytes(1);
        analyzer.reset(toAnalyze);
        Assert.assertTrue(!analyzer.hasNext());
    }
}
