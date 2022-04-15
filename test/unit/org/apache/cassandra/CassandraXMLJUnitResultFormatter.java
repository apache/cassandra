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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Enumeration;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang3.StringUtils;

import ch.qos.logback.classic.spi.ILoggingEvent;
import junit.framework.AssertionFailedError;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.taskdefs.optional.junit.FormatterElement;
import org.apache.tools.ant.taskdefs.optional.junit.IgnoredTestListener;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitResultFormatter;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitTest;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitTestRunner;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitVersionHelper;
import org.apache.tools.ant.taskdefs.optional.junit.XMLConstants;
import org.apache.tools.ant.util.DOMElementWriter;
import org.apache.tools.ant.util.DateUtils;
import org.apache.tools.ant.util.FileUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;

import static org.apache.tools.ant.taskdefs.optional.junit.JUnitTestRunner.getFilteredTrace;
import static org.apache.tools.ant.taskdefs.optional.junit.JUnitVersionHelper.getTestCaseClassName;


/**
 * Prints XML output of the test to a specified Writer.
 *
 * @see FormatterElement
 */

public class CassandraXMLJUnitResultFormatter implements JUnitResultFormatter, XMLConstants, IgnoredTestListener
{
    private static final boolean FAIL_ON_FORBIDDEN_LOG_ENTRIES = Boolean.getBoolean("cassandra.test.fail_on_forbidden_log_entries");
    /**
     * constant for unnnamed testsuites/cases
     */
    private static final String UNKNOWN = "unknown";

    private static DocumentBuilder getDocumentBuilder()
    {
        try
        {
            return DocumentBuilderFactory.newInstance().newDocumentBuilder();
        }
        catch (final Exception exc)
        {
            throw new ExceptionInInitializerError(exc);
        }
    }

    private static final String tag = System.getProperty("cassandra.testtag");

    /**
     * The XML document.
     */
    private Document doc;

    /**
     * The wrapper for the whole testsuite.
     */
    private Element rootElement;

    /**
     * Element for the current test.
     * <p>
     * The keying of this map is a bit of a hack: tests are keyed by caseName(className) since
     * the Test we get for Test-start isn't the same as the Test we get during test-assumption-fail,
     * so we can't easily match Test objects without manually iterating over all keys and checking
     * individual fields.
     */
    private final ConcurrentMap<String, Element> testElements = new ConcurrentHashMap<>();

    /**
     * Tests that failed - see {@link #testElements} for keys interpretation
     */
    private final ConcurrentMap<String, Element> failedTests = new ConcurrentHashMap<>();

    /**
     * Tests that were skipped - see {@link #testElements} for keys interpretation
     */
    private final ConcurrentMap<String, Test> skippedTests = new ConcurrentHashMap<>();

    /**
     * Tests that were ignored - see {@link #testElements} for keys interpretation
     */
    private final ConcurrentMap<String, Test> ignoredTests = new ConcurrentHashMap<>();

    /**
     * Times when the tests were started - see {@link #testElements} for keys interpretation
     */
    private final ConcurrentMap<String, Long> testStarts = new ConcurrentHashMap<>();

    /**
     * Times when the tests were finished - see {@link #testElements} for keys interpretation
     */
    private final ConcurrentMap<String, Long> testEnds = new ConcurrentHashMap<>();

    /**
     * Forbbidden log entries (collected as throwables), recorded before, between and after test cases (for example
     * during execution of @BeforeClass or @AfterClass blocks). Keys are the times in ms of the event
     */
    private final ConcurrentSkipListMap<Long, Throwable> suiteEvents = new ConcurrentSkipListMap<>();

    /**
     * Forbidden log entries (collected as throwawbles) recorded during tests execution - see {@link #suiteEvents} for keys interpretation
     */
    private final ConcurrentSkipListMap<Long, Throwable> testEvents = new ConcurrentSkipListMap<>();

    /**
     * The current events map - the listener registered in {@link ForbiddenLogEntriesFilter} writes the events to the
     * map referenced by this variable
     */
    private volatile ConcurrentSkipListMap<Long, Throwable> events = suiteEvents;

    /**
     * Where to write the log to.
     */
    private OutputStream out;

    private String curSuiteName = null;
    private String classCaseDesc = null;

    private ForbiddenLogEntriesFilter forbiddenLogEntriesFilter;

    /**
     * No arg constructor.
     */
    public CassandraXMLJUnitResultFormatter()
    {
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void setOutput(final OutputStream out)
    {
        this.out = out;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void setSystemOutput(final String out)
    {
        maybeAddClassCaseElement();
        addOutputNode(SYSTEM_OUT, out);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void setSystemError(final String out)
    {
        maybeAddClassCaseElement();
        addOutputNode(SYSTEM_ERR, out);
    }

    /**
     * The whole testsuite started.
     *
     * @param suite the testsuite.
     */
    @Override
    public void startTestSuite(final JUnitTest suite)
    {
        forbiddenLogEntriesFilter = ForbiddenLogEntriesFilter.getInstanceIfUsed();
        if (FAIL_ON_FORBIDDEN_LOG_ENTRIES && forbiddenLogEntriesFilter != null)
            forbiddenLogEntriesFilter.setListener(this::onForbiddenLogEvent);

        long startTime = System.currentTimeMillis();
        curSuiteName = suite.getName();
        classCaseDesc = String.format("%s(%s)", formatName(curSuiteName), TestSuite.class.getName());

        doc = getDocumentBuilder().newDocument();
        rootElement = doc.createElement(TESTSUITE);
        rootElement.setAttribute(ATTR_NAME, formatName(suite.getName()));

        //add the timestamp
        final String timestamp = DateUtils.format(new Date(startTime), DateUtils.ISO8601_DATETIME_PATTERN);
        rootElement.setAttribute(TIMESTAMP, timestamp);
        //and the hostname.
        rootElement.setAttribute(HOSTNAME, getHostname());

        // Output properties
        final Element propsElement = doc.createElement(PROPERTIES);
        rootElement.appendChild(propsElement);
        final Properties props = suite.getProperties();
        if (props != null)
        {
            final Enumeration<?> e = props.propertyNames();
            while (e.hasMoreElements())
            {
                final String name = (String) e.nextElement();
                final Element propElement = doc.createElement(PROPERTY);
                propElement.setAttribute(ATTR_NAME, name);
                propElement.setAttribute(ATTR_VALUE, props.getProperty(name));
                propsElement.appendChild(propElement);
            }
        }
    }


    /**
     * Interface TestListener.
     *
     * <p>A new Test is started.
     *
     * @param test the test.
     */
    @Override
    public void startTest(final Test test)
    {
        testEvents.clear();
        events = testEvents;
        long testStartTime = System.currentTimeMillis();
        String desc = createDescription(test);
        testStarts.put(desc, testStartTime);
    }

    /**
     * Interface TestListener.
     *
     * <p>A Test is finished.
     *
     * @param test the test.
     */
    @Override
    public void endTest(final Test test)
    {
        events = suiteEvents;

        long testEndTime = System.currentTimeMillis();

        final String desc = createDescription(test);

        // Fix for bug #5637 - if a junit.extensions.TestSetup is
        // used and throws an exception during setUp then startTest
        // would never have been called
        if (!testStarts.containsKey(desc))
        {
            testStarts.put(desc, testEndTime);
            testEvents.clear();
        }

        testEnds.put(desc, testEndTime);

        long testStartTime = testStarts.get(desc);
        long testDuration = testEndTime - testStartTime;

        Element currentTest;
        if (!failedTests.containsKey(desc) && !skippedTests.containsKey(desc) && !ignoredTests.containsKey(desc))
        {
            currentTest = createTestCaseElement(getTestCaseClassName(test), resolveCaseName(test), testDuration);
            rootElement.appendChild(currentTest);
            testElements.put(desc, currentTest);
            maybeAddForbiddenEntriesFailureElement(desc, testEvents);
        }
        else
        {
            // the test is skipped / ignored / failed - we do not add another failure because there can be only one
            // failure associated with a test case
            currentTest = testElements.get(desc);
            updateTime(currentTest, testDuration);
        }
    }

    /**
     * The whole testsuite ended.
     *
     * @param suite the testsuite.
     * @throws BuildException on error.
     */
    @Override
    public void endTestSuite(final JUnitTest suite) throws BuildException
    {
        if (FAIL_ON_FORBIDDEN_LOG_ENTRIES && forbiddenLogEntriesFilter != null)
            forbiddenLogEntriesFilter.setListener(null);

        maybeAddClassCaseElement();
        Element classCaseElement = testElements.get(classCaseDesc);
        maybeAddForbiddenEntriesFailureElement(classCaseDesc, suiteEvents);
        long testTime = testStarts.entrySet().stream().mapToLong(entry -> testEnds.getOrDefault(entry.getKey(), entry.getValue()) - entry.getValue()).sum();
        long nonTestTime = Math.max(0L, suite.getRunTime() - testTime);
        updateTime(classCaseElement, nonTestTime);

        for (Map.Entry<String, Element> descAndFailureElem : failedTests.entrySet())
        {
            Element testElem = testElements.get(descAndFailureElem.getKey());
            if (testElem != null)
                testElem.appendChild(descAndFailureElem.getValue());
        }

        rootElement.setAttribute(ATTR_TESTS, String.valueOf(suite.runCount()));
        rootElement.setAttribute(ATTR_FAILURES, String.valueOf(failedTests.size()));
        rootElement.setAttribute(ATTR_ERRORS, String.valueOf(suite.errorCount()));
        rootElement.setAttribute(ATTR_SKIPPED, String.valueOf(suite.skipCount()));
        updateTime(rootElement, suite.getRunTime());
        if (out != null)
        {
            Writer wri = null;
            try
            {
                wri = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
                wri.write("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n");
                (new DOMElementWriter()).write(rootElement, wri, 0, "  ");
            }
            catch (final IOException exc)
            {
                throw new BuildException("Unable to write log file", exc);
            }
            finally
            {
                if (wri != null)
                {
                    try
                    {
                        wri.flush();
                    }
                    catch (final IOException ex)
                    {
                        // ignore
                    }
                }
                if (out != System.out && out != System.err)
                {
                    FileUtils.close(wri);
                }
            }
        }
    }

    /**
     * Interface TestListener for JUnit &gt; 3.4.
     *
     * <p>A Test failed.
     *
     * @param test the test.
     * @param t    the assertion.
     */
    @Override
    public void addFailure(final Test test, final AssertionFailedError t)
    {
        if (test != null)
        {
            endTest(test);
            failedTests.put(createDescription(test), getFailureOrError(t, getFilteredTrace(t)));
        }
    }

    /**
     * Interface TestListener.
     *
     * <p>An error occurred while running the test.
     *
     * @param test the test.
     * @param t    the error.
     */
    @Override
    public void addError(final Test test, final Throwable t)
    {
        if (test != null)
        {
            endTest(test);
            failedTests.put(createDescription(test), getFailureOrError(t, getFilteredTrace(t)));
        }
    }

    @Override
    public void testIgnored(final Test test)
    {
        formatSkip(test, JUnitVersionHelper.getIgnoreMessage(test));
        if (test != null)
        {
            ignoredTests.put(createDescription(test), test);
        }
    }

    @Override
    public void testAssumptionFailure(final Test test, final Throwable failure)
    {
        formatSkip(test, failure.getMessage());
        skippedTests.put(createDescription(test), test);
    }

    private void maybeAddClassCaseElement()
    {
        if (!testElements.containsKey(classCaseDesc))
        {
            Element classElement = createTestCaseElement(TestSuite.class.getName(), formatName(curSuiteName), 0);
            rootElement.appendChild(classElement);
            testElements.put(classCaseDesc, classElement);
        }
    }

    private void addOutputNode(final String type, final String output)
    {
        final Element nested = doc.createElement(type);
        rootElement.appendChild(nested);
        nested.appendChild(doc.createCDATASection(output));
    }

    private String formatName(String name)
    {
        name = name == null ? UNKNOWN : (StringUtils.isBlank(tag) ? name : String.format("%s-%s", name, tag));
        return name;
    }

    private String resolveCaseName(Test test)
    {
        return formatName(JUnitVersionHelper.getTestCaseName(test));
    }

    private String createDescription(final Test test) throws BuildException
    {
        return String.format("%s(%s)", resolveCaseName(test), getTestCaseClassName(test));
    }

    private Element createTestCaseElement(String className, String caseName, long timeMs)
    {
        final Element testCaseElement = doc.createElement(TESTCASE);
        testCaseElement.setAttribute(ATTR_CLASSNAME, className);
        testCaseElement.setAttribute(ATTR_NAME, caseName);
        updateTime(testCaseElement, timeMs);
        return testCaseElement;
    }

    private void updateTime(Element element, long timeMs)
    {
        element.setAttribute(ATTR_TIME, BigDecimal.valueOf(timeMs).divide(BigDecimal.valueOf(1000)).toString());
    }

    private void formatSkip(final Test test, final String message)
    {
        if (test != null)
        {
            endTest(test);
        }

        final Element nested = doc.createElement("skipped");

        if (message != null)
        {
            nested.setAttribute("message", message);
        }

        Element currentTest;
        if (test != null)
        {
            currentTest = testElements.get(createDescription(test));
        }
        else
        {
            currentTest = rootElement;
        }

        currentTest.appendChild(nested);
    }

    private Element getFailureOrError(Throwable t, String content)
    {
        String type = t instanceof AssertionFailedError ? FAILURE : ERROR;
        final Element nested = doc.createElement(type);

        if (t.getMessage() != null && t.getMessage().length() > 0) nested.setAttribute(ATTR_MESSAGE, t.getMessage());
        nested.setAttribute(ATTR_TYPE, t.getClass().getName());

        final Text trace = doc.createTextNode(content);
        nested.appendChild(trace);

        return nested;
    }

    private void maybeAddForbiddenEntriesFailureElement(String desc, NavigableMap<Long, Throwable> forbiddenEntries)
    {
        if (!forbiddenEntries.isEmpty() && !failedTests.containsKey(desc))
        {
            Element elem = getFailureOrError(new AssertionFailedError("Forbidden entries detected"),
                                             forbiddenEntries.values().stream().map(JUnitTestRunner::getFilteredTrace).collect(Collectors.joining("\n", "\n", "")));
            failedTests.putIfAbsent(desc, elem);
            forbiddenEntries.clear();
        }
    }

    private void onForbiddenLogEvent(ILoggingEvent event)
    {
        String timestamp = DateUtils.format(new Date(event.getTimeStamp()), "HH:mm:ss.SSS");
        String msg = String.format("%s %s %s", timestamp, event.getLoggerName(), event.getFormattedMessage());
        Throwable t = new AssertionFailedError(msg);
        t.setStackTrace(event.getCallerData());
        events.put(event.getTimeStamp(), t);
    }

    /**
     * get the local hostname
     *
     * @return the name of the local host, or "localhost" if we cannot work it out
     */
    private String getHostname()
    {
        String hostname = "localhost";
        try
        {
            final InetAddress localHost = InetAddress.getLocalHost();
            if (localHost != null)
            {
                hostname = localHost.getHostName();
            }
        }
        catch (final UnknownHostException e)
        {
            // fall back to default 'localhost'
        }
        return hostname;
    }
} // XMLJUnitResultFormatter
