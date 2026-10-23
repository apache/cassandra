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
package org.apache.cassandra.service;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;

public class ClientWarnTest
{
    private static final String TRUNCATED = " [truncated]";

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void captureWarnings()
    {
        ClientWarn.instance.captureWarnings();
    }

    @After
    public void resetWarnings()
    {
        ClientWarn.instance.resetWarnings();
    }

    @Test
    public void shortWarningIsNotTruncated()
    {
        String warning = "a".repeat(100);
        assertThat(warn(warning)).isEqualTo(warning);
    }

    @Test
    public void warningOfMaxLengthIsNotTruncated()
    {
        String warning = "a".repeat(FBUtilities.MAX_UNSIGNED_SHORT);
        assertThat(warn(warning)).isEqualTo(warning);
    }

    @Test
    public void longAsciiWarningIsTruncated()
    {
        String result = warn("a".repeat(70000));
        assertThat(result).endsWith(TRUNCATED);
        assertThat(CBUtil.encodedUTF8Length(result)).isEqualTo(FBUtilities.MAX_UNSIGNED_SHORT);
    }

    @Test
    public void longMultiByteWarningIsTruncatedByBytes()
    {
        // 40000 chars but 80000 bytes; truncating by chars threw StringIndexOutOfBoundsException
        String result = warn("\u00e9".repeat(40000));
        assertThat(result).endsWith(TRUNCATED);
        assertThat(CBUtil.encodedUTF8Length(result)).isLessThanOrEqualTo(FBUtilities.MAX_UNSIGNED_SHORT);
    }

    @Test
    public void truncationDoesNotSplitSurrogatePair()
    {
        // each emoji is 2 chars and 4 bytes in UTF-8
        String emoji = "\uD83D\uDE00";
        String result = warn(emoji.repeat(20000));
        String kept = result.substring(0, result.length() - TRUNCATED.length());
        assertThat(kept).isEqualTo(emoji.repeat(kept.length() / 2));
        assertThat(CBUtil.encodedUTF8Length(result)).isLessThanOrEqualTo(FBUtilities.MAX_UNSIGNED_SHORT);
    }

    private static String warn(String warning)
    {
        ClientWarn.instance.warn(warning);
        List<String> warnings = ClientWarn.instance.getWarnings();
        assertThat(warnings).hasSize(1);
        return warnings.get(0);
    }
}
