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

package org.apache.cassandra.index.sai.analyzer.filter;

import java.text.Normalizer;

import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;

import static org.junit.Assert.assertEquals;

public class BasicFiltersTest
{
    @Test
    public void testLowerCase()
    {
        BasicFilters.LowerCase lowerCase = new BasicFilters.LowerCase();
        
        for (int count = 0; count < SAITester.getRandom().nextIntBetween(100, 1000); count++)
        {
            String actual = SAITester.getRandom().nextTextString(10, 50);
            assertEquals(actual.toLowerCase(), lowerCase.process(actual));
        }
    }
    
    @Test
    public void testNormalize()
    {
        BasicFilters.Normalize normalize = new BasicFilters.Normalize();

        for (int count = 0; count < SAITester.getRandom().nextIntBetween(100, 1000); count++)
        {
            String actual = SAITester.getRandom().nextTextString(10, 50);
            assertEquals(Normalizer.normalize(actual, Normalizer.Form.NFC), normalize.process(actual));
        }
    }
    
    @Test
    public void testAscii()
    {
        BasicFilters.Ascii ascii = new BasicFilters.Ascii();

        for (int count = 0; count < SAITester.getRandom().nextIntBetween(100, 1000); count++)
        {
            String actual = SAITester.getRandom().nextTextString(100, 5000);

            char[] actualChars = actual.toCharArray();
            char[] expectedChars = new char[actualChars.length * 4];
            int expectedSize = ASCIIFoldingFilter.foldToASCII(actualChars, 0, expectedChars, 0, actualChars.length);
            String expected = new String(expectedChars, 0, expectedSize);

            assertEquals(expected, ascii.process(actual));
        }
    }
}
