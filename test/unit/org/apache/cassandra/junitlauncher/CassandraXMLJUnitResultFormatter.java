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

    package org.apache.cassandra.junitlauncher;

    import java.io.IOException;
    import java.io.OutputStream;
    import java.io.Reader;
    import java.net.InetAddress;
    import java.net.UnknownHostException;
    import java.util.Date;
    import java.util.Map;
    import java.util.Optional;
    import java.util.Properties;
    import java.util.Set;
    import java.util.concurrent.ConcurrentHashMap;
    import java.util.concurrent.atomic.AtomicLong;
    import javax.xml.stream.XMLOutputFactory;
    import javax.xml.stream.XMLStreamException;
    import javax.xml.stream.XMLStreamWriter;

    import org.junit.platform.engine.TestExecutionResult;
    import org.junit.platform.engine.TestSource;
    import org.junit.platform.engine.reporting.ReportEntry;
    import org.junit.platform.engine.support.descriptor.ClassSource;
    import org.junit.platform.launcher.TestIdentifier;
    import org.junit.platform.launcher.TestPlan;

    import com.sun.xml.internal.txw2.output.IndentingXMLStreamWriter;
    import org.apache.tools.ant.util.DOMElementWriter;
    import org.apache.tools.ant.util.DateUtils;
    import org.apache.tools.ant.util.StringUtils;

    /**
     * Prints XML output of the test to a specified Writer. Inspired by
     * {@code org.apache.tools.ant.taskdefs.optional.junitlauncher.LegacyXmlResultFormatter}. We could not extend the
     * existing class, see https://github.com/apache/ant/pull/169 for details. Here is the list of changes compared to the
     *  * original class: 1) test tag support 2) indentation of XML output 3) localhost attribute 4) writing trace with
     *  characters instead of CDATA 5) writing STDOUT/STDERR with CDATA instead of characters 6) better support of 0xA
     *  and 0xD characters in output.
     */
    public class CassandraXMLJUnitResultFormatter extends AbstractJUnitResultFormatter
    {
        private static final double ONE_SECOND = 1000.0;
        private static final String tag = System.getProperty("cassandra.testtag", "");

        private OutputStream outputStream;
        private final Map<TestIdentifier, Stats> testIds = new ConcurrentHashMap<>();
        private final Map<TestIdentifier, Optional<String>> skipped = new ConcurrentHashMap<>();
        private final Map<TestIdentifier, Optional<Throwable>> failed = new ConcurrentHashMap<>();
        private final Map<TestIdentifier, Optional<Throwable>> aborted = new ConcurrentHashMap<>();

        private TestPlan testPlan;
        private long testPlanStartedAt = -1;
        private long testPlanEndedAt = -1;
        private final AtomicLong numTestsRun = new AtomicLong(0);
        private final AtomicLong numTestsFailed = new AtomicLong(0);
        private final AtomicLong numTestsSkipped = new AtomicLong(0);
        private final AtomicLong numTestsAborted = new AtomicLong(0);
        private boolean useLegacyReportingName = true;


        @Override
        public void testPlanExecutionStarted(final TestPlan testPlan)
        {
            this.testPlan = testPlan;
            this.testPlanStartedAt = System.currentTimeMillis();
        }

        @Override
        public void testPlanExecutionFinished(final TestPlan testPlan)
        {
            this.testPlanEndedAt = System.currentTimeMillis();
            // format and print out the result
            try
            {
                new XMLReportWriter().write();
            }
            catch (IOException | XMLStreamException e)
            {
                handleException(e);
            }
        }

        @Override
        public void dynamicTestRegistered(final TestIdentifier testIdentifier)
        {
            // nothing to do
        }

        @Override
        public void executionSkipped(final TestIdentifier testIdentifier, final String reason)
        {
            final long currentTime = System.currentTimeMillis();
            this.numTestsSkipped.incrementAndGet();
            this.skipped.put(testIdentifier, Optional.ofNullable(reason));
            // a skipped test is considered started and ended now
            final Stats stats = new Stats(testIdentifier, currentTime);
            stats.endedAt = currentTime;
            this.testIds.put(testIdentifier, stats);
        }

        @Override
        public void executionStarted(final TestIdentifier testIdentifier)
        {
            final long currentTime = System.currentTimeMillis();
            this.testIds.putIfAbsent(testIdentifier, new Stats(testIdentifier, currentTime));
            if (testIdentifier.isTest())
            {
                this.numTestsRun.incrementAndGet();
            }
        }

        @Override
        public void executionFinished(final TestIdentifier testIdentifier, final TestExecutionResult testExecutionResult)
        {
            final long currentTime = System.currentTimeMillis();
            final Stats stats = this.testIds.get(testIdentifier);
            if (stats != null)
            {
                stats.endedAt = currentTime;
            }
            switch (testExecutionResult.getStatus())
            {
                case SUCCESSFUL:
                {
                    break;
                }
                case ABORTED:
                {
                    this.numTestsAborted.incrementAndGet();
                    this.aborted.put(testIdentifier, testExecutionResult.getThrowable());
                    break;
                }
                case FAILED:
                {
                    this.numTestsFailed.incrementAndGet();
                    this.failed.put(testIdentifier, testExecutionResult.getThrowable());
                    break;
                }
            }
        }

        @Override
        public void reportingEntryPublished(final TestIdentifier testIdentifier, final ReportEntry entry)
        {
            // nothing to do
        }

        @Override
        public void setDestination(final OutputStream os)
        {
            this.outputStream = os;
        }

        @Override
        public void setUseLegacyReportingName(final boolean useLegacyReportingName)
        {
            this.useLegacyReportingName = useLegacyReportingName;
        }

        private String determineTestName(TestIdentifier testIdentifier)
        {
            String testName = this.useLegacyReportingName ? testIdentifier.getLegacyReportingName()
                                                          : testIdentifier.getDisplayName();
            if (!tag.isEmpty())
                testName = testName + '-' + tag;
            return testName;
        }

        private static final class Stats
        {
            @SuppressWarnings({ "unused", "FieldCanBeLocal" })
            private final TestIdentifier testIdentifier;
            private final long startedAt;
            private long endedAt;

            private Stats(final TestIdentifier testIdentifier, final long startedAt)
            {
                this.testIdentifier = testIdentifier;
                this.startedAt = startedAt;
            }
        }

        private final class XMLReportWriter
        {

            private static final String ELEM_TESTSUITE = "testsuite";
            private static final String ELEM_PROPERTIES = "properties";
            private static final String ELEM_PROPERTY = "property";
            private static final String ELEM_TESTCASE = "testcase";
            private static final String ELEM_SKIPPED = "skipped";
            private static final String ELEM_FAILURE = "failure";
            private static final String ELEM_ABORTED = "aborted";
            private static final String ELEM_SYSTEM_OUT = "system-out";
            private static final String ELEM_SYSTEM_ERR = "system-err";


            private static final String ATTR_CLASSNAME = "classname";
            private static final String ATTR_NAME = "name";
            private static final String ATTR_VALUE = "value";
            private static final String ATTR_TIME = "time";
            private static final String ATTR_TIMESTAMP = "timestamp";
            private static final String ATTR_HOSTNAME = "hostname";
            private static final String ATTR_NUM_ABORTED = "aborted";
            private static final String ATTR_NUM_FAILURES = "failures";
            private static final String ATTR_NUM_TESTS = "tests";
            private static final String ATTR_NUM_SKIPPED = "skipped";
            private static final String ATTR_MESSAGE = "message";
            private static final String ATTR_TYPE = "type";

            void write() throws XMLStreamException, IOException
            {
                XMLStreamWriter writer = XMLOutputFactory.newFactory().createXMLStreamWriter(outputStream, "UTF-8");
                writer = new IndentingXMLStreamWriter(writer);
                try
                {
                    writer.writeStartDocument();
                    writeTestSuite(writer);
                    writer.writeEndDocument();
                }
                finally
                {
                    writer.close();
                }
            }

            void writeTestSuite(final XMLStreamWriter writer) throws XMLStreamException, IOException
            {
                // write the testsuite element
                writer.writeStartElement(ELEM_TESTSUITE);
                final String testSuiteName = determineTestSuiteName();
                writeAttribute(writer, ATTR_NAME, testSuiteName);
                // time taken for the tests execution
                writeAttribute(writer, ATTR_TIME, String.valueOf((testPlanEndedAt - testPlanStartedAt) / ONE_SECOND));
                // add the timestamp of report generation
                final String timestamp = DateUtils.format(new Date(), DateUtils.ISO8601_DATETIME_PATTERN);
                writeAttribute(writer, ATTR_TIMESTAMP, timestamp);
                // add hostname where tests were run
                writer.writeAttribute(ATTR_HOSTNAME, getHostName());
                writeAttribute(writer, ATTR_NUM_TESTS, String.valueOf(numTestsRun.longValue()));
                writeAttribute(writer, ATTR_NUM_FAILURES, String.valueOf(numTestsFailed.longValue()));
                writeAttribute(writer, ATTR_NUM_SKIPPED, String.valueOf(numTestsSkipped.longValue()));
                writeAttribute(writer, ATTR_NUM_ABORTED, String.valueOf(numTestsAborted.longValue()));

                // write the properties
                writeProperties(writer);
                // write the tests
                writeTestCase(writer);
                writeSysOut(writer);
                writeSysErr(writer);
                // end the testsuite
                writer.writeEndElement();
            }

            void writeProperties(final XMLStreamWriter writer) throws XMLStreamException
            {
                final Properties properties = CassandraXMLJUnitResultFormatter.this.context.getProperties();
                if (properties == null || properties.isEmpty())
                {
                    return;
                }
                writer.writeStartElement(ELEM_PROPERTIES);
                for (final String prop : properties.stringPropertyNames())
                {
                    writer.writeStartElement(ELEM_PROPERTY);
                    writeAttribute(writer, ATTR_NAME, prop);
                    writeAttribute(writer, ATTR_VALUE, properties.getProperty(prop));
                    writer.writeEndElement();
                }
                writer.writeEndElement();
            }

            void writeTestCase(final XMLStreamWriter writer) throws XMLStreamException
            {
                for (final Map.Entry<TestIdentifier, Stats> entry : testIds.entrySet())
                {
                    final TestIdentifier testId = entry.getKey();
                    if (!testId.isTest() && !failed.containsKey(testId))
                    {
                        // only interested in test methods unless there was a failure,
                        // in which case we want the exception reported
                        // (https://bz.apache.org/bugzilla/show_bug.cgi?id=63850)
                        continue;
                    }
                    // find the associated class of this test
                    final Optional<ClassSource> parentClassSource;
                    if (testId.isTest())
                    {
                        parentClassSource = findFirstParentClassSource(testId);
                    }
                    else
                    {
                        parentClassSource = findFirstClassSource(testId);
                    }
                    if (!parentClassSource.isPresent())
                    {
                        continue;
                    }
                    final String classname = (parentClassSource.get()).getClassName();
                    writer.writeStartElement(ELEM_TESTCASE);
                    writeAttribute(writer, ATTR_CLASSNAME, classname);
                    writeAttribute(writer, ATTR_NAME, determineTestName(testId));
                    final Stats stats = entry.getValue();
                    writeAttribute(writer, ATTR_TIME, String.valueOf((stats.endedAt - stats.startedAt) / ONE_SECOND));
                    // skipped element if the test was skipped
                    writeSkipped(writer, testId);
                    // failed element if the test failed
                    writeFailed(writer, testId);
                    // aborted element if the test was aborted
                    writeAborted(writer, testId);

                    writer.writeEndElement();
                }
            }

            private void writeSkipped(final XMLStreamWriter writer, final TestIdentifier testIdentifier) throws XMLStreamException
            {
                if (!skipped.containsKey(testIdentifier))
                {
                    return;
                }
                writer.writeStartElement(ELEM_SKIPPED);
                final Optional<String> reason = skipped.get(testIdentifier);
                if (reason.isPresent())
                {
                    writeAttribute(writer, ATTR_MESSAGE, reason.get());
                }
                writer.writeEndElement();
            }

            private void writeFailed(final XMLStreamWriter writer, final TestIdentifier testIdentifier) throws XMLStreamException
            {
                if (!failed.containsKey(testIdentifier))
                {
                    return;
                }
                writer.writeStartElement(ELEM_FAILURE);
                final Optional<Throwable> cause = failed.get(testIdentifier);
                if (cause.isPresent())
                {
                    final Throwable t = cause.get();
                    final String message = t.getMessage();
                    if (message != null && !message.trim().isEmpty())
                    {
                        writeAttribute(writer, ATTR_MESSAGE, message);
                    }
                    writeAttribute(writer, ATTR_TYPE, t.getClass().getName());
                    // write out the stacktrace
                    writer.writeCharacters(StringUtils.getStackTrace(t));
                }
                writer.writeEndElement();
            }

            private void writeAborted(final XMLStreamWriter writer, final TestIdentifier testIdentifier) throws XMLStreamException
            {
                if (!aborted.containsKey(testIdentifier))
                {
                    return;
                }
                writer.writeStartElement(ELEM_ABORTED);
                final Optional<Throwable> cause = aborted.get(testIdentifier);
                if (cause.isPresent())
                {
                    final Throwable t = cause.get();
                    final String message = t.getMessage();
                    if (message != null && !message.trim().isEmpty())
                    {
                        writeAttribute(writer, ATTR_MESSAGE, message);
                    }
                    writeAttribute(writer, ATTR_TYPE, t.getClass().getName());
                    // write out the stacktrace
                    writer.writeCData(StringUtils.getStackTrace(t));
                }
                writer.writeEndElement();
            }

            private void writeSysOut(final XMLStreamWriter writer) throws XMLStreamException, IOException
            {
                if (!CassandraXMLJUnitResultFormatter.this.hasSysOut())
                {
                    return;
                }
                writer.writeStartElement(ELEM_SYSTEM_OUT);
                try (final Reader reader = CassandraXMLJUnitResultFormatter.this.getSysOutReader())
                {
                    writeCDataFrom(reader, writer);
                }
                writer.writeEndElement();
            }

            private void writeSysErr(final XMLStreamWriter writer) throws XMLStreamException, IOException
            {
                if (!CassandraXMLJUnitResultFormatter.this.hasSysErr())
                    return;
                writer.writeStartElement(ELEM_SYSTEM_ERR);
                try (final Reader reader = CassandraXMLJUnitResultFormatter.this.getSysErrReader())
                {
                    writeCDataFrom(reader, writer);
                }
                writer.writeEndElement();
            }

            private void writeCDataFrom(final Reader reader, final XMLStreamWriter writer) throws IOException, XMLStreamException
            {
                final char[] chars = new char[1024];
                int numRead;
                while ((numRead = reader.read(chars)) != -1)
                {
                    writer.writeCData(encode(new String(chars, 0, numRead)));
                }
            }

            private void writeAttribute(final XMLStreamWriter writer, final String name, final String value) throws XMLStreamException
            {
                writer.writeAttribute(name, encode(value));
            }

            private String encode(final String s)
            {
                boolean changed = false;
                final StringBuilder sb = new StringBuilder();
                for (char c : s.toCharArray())
                {
                    if (!DOMElementWriter.isLegalXmlCharacter(c))
                    {
                        changed = true;
                        sb.append("&#").append((int) c).append(';');
                    }
                    if (c == 0xA || c == 0xD)
                    {
                        changed = true;
                        sb.append("&#x").append(Integer.toHexString(c)).append(';');
                    }
                    else
                    {
                        sb.append(c);
                    }
                }
                return changed ? sb.toString() : s;
            }

            private String determineTestSuiteName()
            {
                // this is really a hack to try and match the expectations of the XML report in JUnit4.x
                // world. In JUnit5, the TestPlan doesn't have a name and a TestPlan (for which this is a
                // listener) can have numerous tests within it
                final Set<TestIdentifier> roots = testPlan.getRoots();
                if (roots.isEmpty())
                {
                    return "UNKNOWN";
                }
                for (final TestIdentifier root : roots)
                {
                    final Optional<ClassSource> classSource = findFirstClassSource(root);
                    if (classSource.isPresent())
                    {
                        String className = classSource.get().getClassName();
                        if (!tag.isEmpty())
                        {
                            return className + '-' + tag;
                        }
                        else
                        {
                            return className;
                        }
                    }
                }
                return "UNKNOWN";
            }

            private Optional<ClassSource> findFirstClassSource(final TestIdentifier root)
            {
                if (root.getSource().isPresent())
                {
                    final TestSource source = root.getSource().get();
                    if (source instanceof ClassSource)
                    {
                        return Optional.of((ClassSource) source);
                    }
                }
                for (final TestIdentifier child : testPlan.getChildren(root))
                {
                    final Optional<ClassSource> classSource = findFirstClassSource(child);
                    if (classSource.isPresent())
                    {
                        return classSource;
                    }
                }
                return Optional.empty();
            }

            private Optional<ClassSource> findFirstParentClassSource(final TestIdentifier testId)
            {
                final Optional<TestIdentifier> parent = testPlan.getParent(testId);
                if (!parent.isPresent())
                {
                    return Optional.empty();
                }
                if (!parent.get().getSource().isPresent())
                {
                    // the source of the parent is unknown, so we move up the
                    // hierarchy and try and find a class source
                    return findFirstParentClassSource(parent.get());
                }
                final TestSource parentSource = parent.get().getSource().get();
                return parentSource instanceof ClassSource ? Optional.of((ClassSource) parentSource)
                                                           : findFirstParentClassSource(parent.get());
            }

            /**
             * Get the local hostname.
             *
             * @return the name of the local host, or "localhost" if we cannot find it out
             */
            private String getHostName()
            {
                String hostname = "localhost";
                try
                {
                    final InetAddress localHost = InetAddress.getLocalHost();
                    if (localHost != null)
                        hostname = localHost.getHostName();
                }
                catch (final UnknownHostException e)
                {
                    // fall back to default 'localhost'
                }
                return hostname;
            }
        }
    }
