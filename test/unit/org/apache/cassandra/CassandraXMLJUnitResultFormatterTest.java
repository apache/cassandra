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

package org.apache.cassandra;

import java.io.ByteArrayOutputStream;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.tools.ant.taskdefs.optional.junit.JUnitTest;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import junit.framework.AssertionFailedError; // checkstyle: permit this import

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CassandraXMLJUnitResultFormatterTest
{
    private static final DocumentBuilderFactory DOCUMENT_BUILDER_FACTORY = DocumentBuilderFactory.newInstance();

    // Minimal junit.framework.Test implementation used to drive the formatter.
    private static class FakeTest implements junit.framework.Test
    {
        private final String className;
        private final String methodName;

        FakeTest(String className, String methodName)
        {
            this.className = className;
            this.methodName = methodName;
        }

        @Override
        public int countTestCases() { return 1; }

        @Override
        public void run(junit.framework.TestResult result) {}

        // Providing toString() in JUnit3 name-extraction format: "methodName(className)"
        @Override
        public String toString() { return methodName + "(" + className + ")"; }
    }

    private CassandraXMLJUnitResultFormatter formatter;
    private ByteArrayOutputStream out;

    @Before
    public void setUp()
    {
        formatter = new CassandraXMLJUnitResultFormatter();
        out = new ByteArrayOutputStream();
        formatter.setOutput(out);
        JUnitTest suite = new JUnitTest("org.apache.cassandra.SomeTest");
        formatter.startTestSuite(suite);
    }

    /**
     * Regression test for CASSANDRA-21396: when both addFailure and addError are called for the
     * same test (e.g. assertion failure in test body + error during teardown), the formatter must
     * emit exactly one status child element under testcase section rather than two.
     * Having two status children violates the JUnit XML spec and breaks ci_summary.html generation.
     * Both messages must be preserved in the surviving element's message attribute.
     */
    @Test
    public void testFailureThenErrorProducesOneStatusChild() throws Exception
    {
        FakeTest test = new FakeTest("org.apache.cassandra.SomeTest", "someTestMethod");

        formatter.startTest(test);
        formatter.addFailure(test, new AssertionFailedError("first failure message"));
        formatter.addError(test, new RuntimeException("second error message"));
        formatter.endTest(test);

        JUnitTest suite = new JUnitTest("org.apache.cassandra.SomeTest");
        suite.setCounts(1, 1, 1, 0);
        formatter.endTestSuite(suite);

        Document doc = parseOutput();
        NodeList testcases = doc.getElementsByTagName("testcase");
        assertEquals("Expected exactly one testcase element", 1, testcases.getLength());

        Element testcase = (Element) testcases.item(0);
        NodeList failures = testcase.getElementsByTagName("failure");
        NodeList errors = testcase.getElementsByTagName("error");
        int totalStatusChildren = failures.getLength() + errors.getLength();
        assertEquals("Expected exactly one status child element (failure or error) per testcase", 1, totalStatusChildren);

        Element statusElem = (Element) (failures.getLength() > 0 ? failures.item(0) : errors.item(0));
        String message = statusElem.getAttribute("message");
        assertTrue("message should contain first failure text", message.contains("first failure message"));
        assertTrue("message should contain second error text", message.contains("second error message"));
    }

    @Test
    public void testSingleFailureIsUnchanged() throws Exception
    {
        FakeTest test = new FakeTest("org.apache.cassandra.SomeTest", "someTestMethod");

        formatter.startTest(test);
        formatter.addFailure(test, new AssertionFailedError("only failure"));
        formatter.endTest(test);

        JUnitTest suite = new JUnitTest("org.apache.cassandra.SomeTest");
        suite.setCounts(1, 1, 0, 0);
        formatter.endTestSuite(suite);

        Document doc = parseOutput();
        Element testcase = (Element) doc.getElementsByTagName("testcase").item(0);
        NodeList failures = testcase.getElementsByTagName("failure");
        NodeList errors = testcase.getElementsByTagName("error");

        assertEquals("Expected one failure element", 1, failures.getLength());
        assertEquals("Expected no error element", 0, errors.getLength());
        assertEquals("only failure", ((Element) failures.item(0)).getAttribute("message"));
    }

    @Test
    public void testPassedTestHasNoStatusChild() throws Exception
    {
        FakeTest test = new FakeTest("org.apache.cassandra.SomeTest", "someTestMethod");

        formatter.startTest(test);
        formatter.endTest(test);

        JUnitTest suite = new JUnitTest("org.apache.cassandra.SomeTest");
        suite.setCounts(1, 0, 0, 0);
        formatter.endTestSuite(suite);

        Document doc = parseOutput();
        Element testcase = (Element) doc.getElementsByTagName("testcase").item(0);
        NodeList failures = testcase.getElementsByTagName("failure");
        NodeList errors = testcase.getElementsByTagName("error");
        NodeList skipped = testcase.getElementsByTagName("skipped");

        assertEquals(0, failures.getLength());
        assertEquals(0, errors.getLength());
        assertEquals(0, skipped.getLength());
    }

    private Document parseOutput() throws Exception
    {
        return DOCUMENT_BUILDER_FACTORY.newDocumentBuilder().parse(new java.io.ByteArrayInputStream(out.toByteArray()));
    }
}
