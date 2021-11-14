/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.tools.ant.taskdefs.optional.junitlauncher;

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.reporting.ReportEntry;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/**
 * A {@link TestResultFormatter} which prints a short statistic for each of the tests
 */
class LegacyPlainResultFormatter extends AbstractJUnitResultFormatter implements TestResultFormatter {

    private OutputStream outputStream;
    private final Map<TestIdentifier, Stats> testIds = new ConcurrentHashMap<>();
    private TestPlan testPlan;
    private BufferedWriter writer;
    private boolean useLegacyReportingName = true;

    @Override
    public void testPlanExecutionStarted(final TestPlan testPlan) {
        this.testPlan = testPlan;
    }

    @Override
    public void testPlanExecutionFinished(final TestPlan testPlan) {
        for (final Map.Entry<TestIdentifier, Stats> entry : this.testIds.entrySet()) {
            final TestIdentifier testIdentifier = entry.getKey();
            if (!isTestClass(testIdentifier).isPresent()) {
                // we are not interested in anything other than a test "class" in this section
                continue;
            }
            final Stats stats = entry.getValue();
            final StringBuilder sb = new StringBuilder("Tests run: ").append(stats.numTestsRun.get());
            sb.append(", Failures: ").append(stats.numTestsFailed.get());
            sb.append(", Skipped: ").append(stats.numTestsSkipped.get());
            sb.append(", Aborted: ").append(stats.numTestsAborted.get());
            sb.append(", Time elapsed: ");
            stats.appendElapsed(sb);
            try {
                this.writer.write(sb.toString());
                this.writer.newLine();
            } catch (IOException ioe) {
                handleException(ioe);
                return;
            }
        }
        // write out sysout and syserr content if any
        try {
            if (this.hasSysOut()) {
                this.writer.write("------------- Standard Output ---------------");
                this.writer.newLine();
                writeSysOut(writer);
                this.writer.write("------------- ---------------- ---------------");
                this.writer.newLine();
            }
            if (this.hasSysErr()) {
                this.writer.write("------------- Standard Error ---------------");
                this.writer.newLine();
                writeSysErr(writer);
                this.writer.write("------------- ---------------- ---------------");
                this.writer.newLine();
            }
        } catch (IOException ioe) {
            handleException(ioe);
        }
    }

    @Override
    public void dynamicTestRegistered(final TestIdentifier testIdentifier) {
        // nothing to do
    }

    @Override
    public void executionSkipped(final TestIdentifier testIdentifier, final String reason) {
        final long currentTime = System.currentTimeMillis();
        this.testIds.putIfAbsent(testIdentifier, new Stats(testIdentifier, currentTime));
        final Stats stats = this.testIds.get(testIdentifier);
        stats.setEndedAt(currentTime);
        if (testIdentifier.isTest()) {
            final StringBuilder sb = new StringBuilder();
            sb.append("Test: ");
            sb.append(this.useLegacyReportingName ? testIdentifier.getLegacyReportingName() : testIdentifier.getDisplayName());
            sb.append(" took ");
            stats.appendElapsed(sb);
            sb.append(" SKIPPED");
            if (reason != null && !reason.isEmpty()) {
                sb.append(": ").append(reason);
            }
            try {
                this.writer.write(sb.toString());
                this.writer.newLine();
            } catch (IOException ioe) {
                handleException(ioe);
                return;
            }
        }
        // get the parent test class to which this skipped test belongs to
        final Optional<TestIdentifier> parentTestClass = traverseAndFindTestClass(this.testPlan, testIdentifier);
        if (!parentTestClass.isPresent()) {
            return;
        }
        final Stats parentClassStats = this.testIds.get(parentTestClass.get());
        parentClassStats.numTestsSkipped.incrementAndGet();
    }

    @Override
    public void executionStarted(final TestIdentifier testIdentifier) {
        final long currentTime = System.currentTimeMillis();
        // record this testidentifier's start
        this.testIds.putIfAbsent(testIdentifier, new Stats(testIdentifier, currentTime));
        final Optional<ClassSource> testClass = isTestClass(testIdentifier);
        if (testClass.isPresent()) {
            // if this is a test class, then print it out
            try {
                this.writer.write("Testcase: " + testClass.get().getClassName());
                this.writer.newLine();
            } catch (IOException ioe) {
                handleException(ioe);
                return;
            }
        }
        // if this is a test (method) then increment the tests run for the test class to which
        // this test belongs to
        if (testIdentifier.isTest()) {
            final Optional<TestIdentifier> parentTestClass = traverseAndFindTestClass(this.testPlan, testIdentifier);
            if (parentTestClass.isPresent()) {
                final Stats parentClassStats = this.testIds.get(parentTestClass.get());
                if (parentClassStats != null) {
                    parentClassStats.numTestsRun.incrementAndGet();
                }
            }
        }
    }

    @SuppressWarnings("incomplete-switch")
    @Override
    public void executionFinished(final TestIdentifier testIdentifier, final TestExecutionResult testExecutionResult) {
        final long currentTime = System.currentTimeMillis();
        final Stats stats = this.testIds.get(testIdentifier);
        if (stats != null) {
            stats.setEndedAt(currentTime);
        }
        if (testIdentifier.isTest() && shouldReportExecutionFinished(testIdentifier, testExecutionResult)) {
            final StringBuilder sb = new StringBuilder();
            sb.append("Test: ");
            sb.append(this.useLegacyReportingName ? testIdentifier.getLegacyReportingName() : testIdentifier.getDisplayName());
            if (stats != null) {
                sb.append(" took ");
                stats.appendElapsed(sb);
            }
            switch (testExecutionResult.getStatus()) {
                case ABORTED: {
                    sb.append(" ABORTED");
                    appendThrowable(sb, testExecutionResult);
                    break;
                }
                case FAILED: {
                    sb.append(" FAILED");
                    appendThrowable(sb, testExecutionResult);
                    break;
                }
            }
            try {
                this.writer.write(sb.toString());
                this.writer.newLine();
            } catch (IOException ioe) {
                handleException(ioe);
                return;
            }
        }
        // get the parent test class in which this test completed
        final Optional<TestIdentifier> parentTestClass = traverseAndFindTestClass(this.testPlan, testIdentifier);
        if (!parentTestClass.isPresent()) {
            return;
        }
        // update the stats of the parent test class
        final Stats parentClassStats = this.testIds.get(parentTestClass.get());
        switch (testExecutionResult.getStatus()) {
            case ABORTED: {
                parentClassStats.numTestsAborted.incrementAndGet();
                break;
            }
            case FAILED: {
                parentClassStats.numTestsFailed.incrementAndGet();
                break;
            }
        }
    }

    @Override
    public void reportingEntryPublished(final TestIdentifier testIdentifier, final ReportEntry entry) {
        // nothing to do
    }

    @Override
    public void setDestination(final OutputStream os) {
        this.outputStream = os;
        this.writer = new BufferedWriter(new OutputStreamWriter(this.outputStream, StandardCharsets.UTF_8));
    }

    @Override
    public void setUseLegacyReportingName(final boolean useLegacyReportingName) {
        this.useLegacyReportingName = useLegacyReportingName;
    }

    protected boolean shouldReportExecutionFinished(final TestIdentifier testIdentifier, final TestExecutionResult testExecutionResult) {
        return true;
    }

    private static void appendThrowable(final StringBuilder sb, TestExecutionResult result) {
        if (!result.getThrowable().isPresent()) {
            return;
        }
        final Throwable throwable = result.getThrowable().get();
        sb.append(String.format(": %s%n", throwable.getMessage()));
        final StringWriter stacktrace = new StringWriter();
        throwable.printStackTrace(new PrintWriter(stacktrace));
        sb.append(stacktrace.toString());
    }

    @Override
    public void close() throws IOException {
        if (this.writer != null) {
            this.writer.close();
        }
        super.close();
    }

    private final class Stats {
        @SuppressWarnings("unused")
        private final TestIdentifier testIdentifier;
        private final AtomicLong numTestsRun = new AtomicLong(0);
        private final AtomicLong numTestsFailed = new AtomicLong(0);
        private final AtomicLong numTestsSkipped = new AtomicLong(0);
        private final AtomicLong numTestsAborted = new AtomicLong(0);
        private final long startedAt;
        private long endedAt;

        private Stats(final TestIdentifier testIdentifier, final long startedAt) {
            this.testIdentifier = testIdentifier;
            this.startedAt = startedAt;
        }

        private void setEndedAt(final long endedAt) {
            this.endedAt = endedAt;
        }

        private void appendElapsed(StringBuilder sb) {
            final long timeElapsed = endedAt - startedAt;
            if (timeElapsed < 1000) {
                sb.append(timeElapsed).append(" milli sec(s)");
            } else {
                sb.append(TimeUnit.SECONDS.convert(timeElapsed, TimeUnit.MILLISECONDS)).append(" sec(s)");
            }
        }
    }
}
