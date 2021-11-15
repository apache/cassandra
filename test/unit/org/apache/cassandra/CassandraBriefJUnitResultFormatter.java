/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.cassandra;

import org.junit.platform.launcher.TestIdentifier;

import org.apache.tools.ant.taskdefs.optional.junitlauncher2.LegacyBriefResultFormatter;

/**
 * Prints plain text output of the test to a specified Writer.
 */
public class CassandraBriefJUnitResultFormatter extends LegacyBriefResultFormatter
{
    private static final String tag = System.getProperty("cassandra.testtag", "");
    private static final Boolean keepBriefBrief = Boolean.getBoolean("cassandra.keepBriefBrief");

    @Override
    protected String determineTestName(TestIdentifier testId)
    {
        String testName = super.determineTestName(testId);
        if (!tag.isEmpty())
            testName = testName + '-' + tag;
        return testName;
    }

    @Override
    protected String determineTestSuiteName()
    {
        String testSuiteName = super.determineTestSuiteName();
        if (!"UNKNOWN".equals(testSuiteName) && !tag.isEmpty())
            testSuiteName = testSuiteName + '-' + tag;
        return testSuiteName;
    }

    @Override
    protected boolean hasSysOut()
    {
        return !keepBriefBrief && super.hasSysOut();
    }

    @Override
    protected boolean hasSysErr()
    {
        return !keepBriefBrief && super.hasSysErr();
    }
}
