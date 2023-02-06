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
import java.io.StringWriter;
import java.text.NumberFormat;

import junit.framework.AssertionFailedError;  // checkstyle: permit this import
import junit.framework.Test;  // checkstyle: permit this import

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.taskdefs.optional.junit.IgnoredTestListener;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitResultFormatter;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitTest;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitTestRunner;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitVersionHelper;
import org.apache.tools.ant.util.FileUtils;
import org.apache.tools.ant.util.StringUtils;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_CASSANDRA_KEEPBRIEFBRIEF;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_CASSANDRA_TESTTAG;

/**
 * Prints plain text output of the test to a specified Writer.
 * Inspired by the PlainJUnitResultFormatter.
 *
 * @see FormatterElement
 * @see PlainJUnitResultFormatter
 */
public class CassandraBriefJUnitResultFormatter implements JUnitResultFormatter, IgnoredTestListener {

    private static final double ONE_SECOND = 1000.0;

    private static final String tag = TEST_CASSANDRA_TESTTAG.getString();

    private static final Boolean keepBriefBrief = TEST_CASSANDRA_KEEPBRIEFBRIEF.getBoolean();

    /**
     * Where to write the log to.
     */
    private OutputStream out;

    /**
     * Used for writing the results.
     */
    private BufferedWriter output;

    /**
     * Used as part of formatting the results.
     */
    private StringWriter results;

    /**
     * Used for writing formatted results to.
     */
    private BufferedWriter resultWriter;

    /**
     * Formatter for timings.
     */
    private NumberFormat numberFormat = NumberFormat.getInstance();

    /**
     * Output suite has written to System.out
     */
    private String systemOutput = null;

    /**
     * Output suite has written to System.err
     */
    private String systemError = null;

    /**
     * Constructor for BriefJUnitResultFormatter.
     */
    public CassandraBriefJUnitResultFormatter() {
        results = new StringWriter();
        resultWriter = new BufferedWriter(results);
    }

    /**
     * Sets the stream the formatter is supposed to write its results to.
     * @param out the output stream to write to
     */
    public void setOutput(OutputStream out) {
        this.out = out;
        output = new BufferedWriter(new java.io.OutputStreamWriter(out));
    }

    /**
     * @see JUnitResultFormatter#setSystemOutput(String)
     */
    /** {@inheritDoc}. */
    public void setSystemOutput(String out) {
        systemOutput = out;
    }

    /**
     * @see JUnitResultFormatter#setSystemError(String)
     */
    /** {@inheritDoc}. */
    public void setSystemError(String err) {
        systemError = err;
    }


    /**
     * The whole testsuite started.
     * @param suite the test suite
     */
    public void startTestSuite(JUnitTest suite) {
        if (output == null) {
            return; // Quick return - no output do nothing.
        }
        StringBuffer sb = new StringBuffer("Testsuite: ");
        String n = suite.getName();
        if (n != null && !tag.isEmpty())
            n = n + "-" + tag;
        sb.append(n);
        sb.append(StringUtils.LINE_SEP);
        try {
            output.write(sb.toString());
            output.flush();
        } catch (IOException ex) {
            throw new BuildException(ex);
        }
    }

    /**
     * The whole testsuite ended.
     * @param suite the test suite
     */
    public void endTestSuite(JUnitTest suite) {
        StringBuffer sb = new StringBuffer("Testsuite: ");
        String n = suite.getName();
        if (n != null && !tag.isEmpty())
            n = n + "-" + tag;
        sb.append(n);
        sb.append(" Tests run: ");
        sb.append(suite.runCount());
        sb.append(", Failures: ");
        sb.append(suite.failureCount());
        sb.append(", Errors: ");
        sb.append(suite.errorCount());
        sb.append(", Skipped: ");
        sb.append(suite.skipCount());
        sb.append(", Time elapsed: ");
        sb.append(numberFormat.format(suite.getRunTime() / ONE_SECOND));
        sb.append(" sec");
        sb.append(StringUtils.LINE_SEP);
        sb.append(StringUtils.LINE_SEP);

        // append the err and output streams to the log
        if (!keepBriefBrief && systemOutput != null && systemOutput.length() > 0) {
            sb.append("------------- Standard Output ---------------")
                    .append(StringUtils.LINE_SEP)
                    .append(systemOutput)
                    .append("------------- ---------------- ---------------")
                    .append(StringUtils.LINE_SEP);
        }

        if (!keepBriefBrief && systemError != null && systemError.length() > 0) {
            sb.append("------------- Standard Error -----------------")
                    .append(StringUtils.LINE_SEP)
                    .append(systemError)
                    .append("------------- ---------------- ---------------")
                    .append(StringUtils.LINE_SEP);
        }

        if (output != null) {
            try {
                output.write(sb.toString());
                resultWriter.close();
                output.write(results.toString());
            } catch (IOException ex) {
                throw new BuildException(ex);
            } finally {
                try {
                    output.flush();
                } catch (IOException ex) {
                    // swallow, there has likely been an exception before this
                }
                if (out != System.out && out != System.err) {
                    FileUtils.close(out);
                }
            }
        }
    }

    /**
     * A test started.
     * @param test a test
     */
    public void startTest(Test test) {
    }

    /**
     * A test ended.
     * @param test a test
     */
    public void endTest(Test test) {
    }

    /**
     * Interface TestListener for JUnit &lt;= 3.4.
     *
     * <p>A Test failed.
     * @param test a test
     * @param t    the exception thrown by the test
     */
    public void addFailure(Test test, Throwable t) {
        formatError("\tFAILED", test, t);
    }

    /**
     * Interface TestListener for JUnit &gt; 3.4.
     *
     * <p>A Test failed.
     * @param test a test
     * @param t    the assertion failed by the test
     */
    public void addFailure(Test test, AssertionFailedError t) {
        addFailure(test, (Throwable) t);
    }

    /**
     * A test caused an error.
     * @param test  a test
     * @param error the error thrown by the test
     */
    public void addError(Test test, Throwable error) {
        formatError("\tCaused an ERROR", test, error);
    }

    /**
     * Format the test for printing..
     * @param test a test
     * @return the formatted testname
     */
    protected String formatTest(Test test) {
        if (test == null) {
            return "Null Test: ";
        } else {
            if (!tag.isEmpty())
                return "Testcase: " + test.toString() + "-" + tag + ":";
            return "Testcase: " + test.toString() + ":";
        }
    }

    /**
     * Format an error and print it.
     * @param type the type of error
     * @param test the test that failed
     * @param error the exception that the test threw
     */
    protected synchronized void formatError(String type, Test test,
                                            Throwable error) {
        if (test != null) {
            endTest(test);
        }

        try {
            resultWriter.write(formatTest(test) + type);
            resultWriter.newLine();
            resultWriter.write(String.valueOf(error.getMessage()));
            resultWriter.newLine();
            String strace = JUnitTestRunner.getFilteredTrace(error);
            resultWriter.write(strace);
            resultWriter.newLine();
            resultWriter.newLine();
        } catch (IOException ex) {
            throw new BuildException(ex);
        }
    }


    public void testIgnored(Test test) {
        formatSkip(test, JUnitVersionHelper.getIgnoreMessage(test));
    }


    public void formatSkip(Test test, String message) {
        if (test != null) {
            endTest(test);
        }

        try {
            resultWriter.write(formatTest(test) + "SKIPPED");
            if (message != null) {
                resultWriter.write(": ");
                resultWriter.write(message);
            }
            resultWriter.newLine();
        } catch (IOException ex) {
            throw new BuildException(ex);
        }

    }

    public void testAssumptionFailure(Test test, Throwable cause) {
        formatSkip(test, cause.getMessage());
    }
}
