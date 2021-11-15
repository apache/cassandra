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
package org.apache.tools.ant.taskdefs.optional.junitlauncher2;

import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.optional.junitlauncher.TestExecutionContext;
import org.apache.tools.ant.taskdefs.optional.junitlauncher.TestResultFormatter;
import org.apache.tools.ant.util.FileUtils;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Contains some common behaviour that's used by our internal {@link TestResultFormatter}s
 */
abstract class AbstractJUnitResultFormatter implements TestResultFormatter {

    protected TestExecutionContext context;
    protected TestPlan testPlan;
    protected boolean useLegacyReportingName = true;

    private SysOutErrContentStore sysOutStore;
    private SysOutErrContentStore sysErrStore;

    @Override
    public void testPlanExecutionStarted(final TestPlan testPlan) {
        this.testPlan = testPlan;
    }

    @Override
    public void sysOutAvailable(final byte[] data) {
        if (this.sysOutStore == null) {
            this.sysOutStore = new SysOutErrContentStore(context, true);
        }
        try {
            this.sysOutStore.store(data);
        } catch (IOException e) {
            handleException(e);
        }
    }

    @Override
    public void sysErrAvailable(final byte[] data) {
        if (this.sysErrStore == null) {
            this.sysErrStore = new SysOutErrContentStore(context, false);
        }
        try {
            this.sysErrStore.store(data);
        } catch (IOException e) {
            handleException(e);
        }
    }

    @Override
    public void setContext(final TestExecutionContext context) {
        this.context = context;
    }

    /**
     * @return Returns true if there's any stdout data, that was generated during the
     * tests, is available for use. Else returns false.
     */
    protected boolean hasSysOut() {
        return this.sysOutStore != null && this.sysOutStore.hasData();
    }

    /**
     * @return Returns true if there's any stderr data, that was generated during the
     * tests, is available for use. Else returns false.
     */
    protected boolean hasSysErr() {
        return this.sysErrStore != null && this.sysErrStore.hasData();
    }

    /**
     * @return Returns a {@link Reader} for reading any stdout data that was generated
     * during the test execution. It is expected that the {@link #hasSysOut()} be first
     * called to see if any such data is available and only if there is, then this method
     * be called
     * @throws IOException If there's any I/O problem while creating the {@link Reader}
     */
    protected Reader getSysOutReader() throws IOException {
        return this.sysOutStore.getReader();
    }

    /**
     * @return Returns a {@link Reader} for reading any stderr data that was generated
     * during the test execution. It is expected that the {@link #hasSysErr()} be first
     * called to see if any such data is available and only if there is, then this method
     * be called
     * @throws IOException If there's any I/O problem while creating the {@link Reader}
     */
    protected Reader getSysErrReader() throws IOException {
        return this.sysErrStore.getReader();
    }

    /**
     * Writes out any stdout data that was generated during the
     * test execution. If there was no such data then this method just returns.
     *
     * @param writer The {@link Writer} to use. Cannot be null.
     * @throws IOException If any I/O problem occurs during writing the data
     */
    void writeSysOut(final Writer writer) throws IOException {
        Objects.requireNonNull(writer, "Writer cannot be null");
        this.writeFrom(this.sysOutStore, writer);
    }

    /**
     * Writes out any stderr data that was generated during the
     * test execution. If there was no such data then this method just returns.
     *
     * @param writer The {@link Writer} to use. Cannot be null.
     * @throws IOException If any I/O problem occurs during writing the data
     */
    protected void writeSysErr(final Writer writer) throws IOException {
        Objects.requireNonNull(writer, "Writer cannot be null");
        this.writeFrom(this.sysErrStore, writer);
    }

    static Optional<TestIdentifier> traverseAndFindTestClass(final TestPlan testPlan, final TestIdentifier testIdentifier) {
        if (isTestClass(testIdentifier).isPresent()) {
            return Optional.of(testIdentifier);
        }
        final Optional<TestIdentifier> parent = testPlan.getParent(testIdentifier);
        return parent.isPresent() ? traverseAndFindTestClass(testPlan, parent.get()) : Optional.empty();
    }

    static Optional<ClassSource> isTestClass(final TestIdentifier testIdentifier) {
        if (testIdentifier == null) {
            return Optional.empty();
        }
        final Optional<TestSource> source = testIdentifier.getSource();
        if (!source.isPresent()) {
            return Optional.empty();
        }
        final TestSource testSource = source.get();
        if (testSource instanceof ClassSource) {
            return Optional.of((ClassSource) testSource);
        }
        return Optional.empty();
    }

    private void writeFrom(final SysOutErrContentStore store, final Writer writer) throws IOException {
        final char[] chars = new char[1024];
        int numRead = -1;
        try (final Reader reader = store.getReader()) {
            while ((numRead = reader.read(chars)) != -1) {
                writer.write(chars, 0, numRead);
            }
        }
    }

    @Override
    public void setUseLegacyReportingName(final boolean useLegacyReportingName) {
        this.useLegacyReportingName = useLegacyReportingName;
    }

    protected String determineTestName(TestIdentifier testId) {
        return useLegacyReportingName ? testId.getLegacyReportingName()
                                      : testId.getDisplayName();
    }

    @Override
    public void close() throws IOException {
        FileUtils.close(this.sysOutStore);
        FileUtils.close(this.sysErrStore);
    }

    protected void handleException(final Throwable t) {
        // we currently just log it and move on.
        this.context.getProject().ifPresent((p) -> p.log("Exception in listener "
                                                         + AbstractJUnitResultFormatter.this.getClass().getName(), t, Project.MSG_DEBUG));
    }

    protected String determineTestSuiteName() {
        // this is really a hack to try and match the expectations of the XML report in JUnit4.x
        // world. In JUnit5, the TestPlan doesn't have a name and a TestPlan (for which this is a
        // listener) can have numerous tests within it
        final Set<TestIdentifier> roots = testPlan.getRoots();
        if (roots.isEmpty()) {
            return "UNKNOWN";
        }
        for (final TestIdentifier root : roots) {
            final Optional<ClassSource> classSource = findFirstClassSource(root);
            if (classSource.isPresent()) {
                return classSource.get().getClassName();
            }
        }
        return "UNKNOWN";
    }

    protected Optional<ClassSource> findFirstClassSource(final TestIdentifier root) {
        if (root.getSource().isPresent()) {
            final TestSource source = root.getSource().get();
            if (source instanceof ClassSource) {
                return Optional.of((ClassSource) source);
            }
        }
        for (final TestIdentifier child : testPlan.getChildren(root)) {
            final Optional<ClassSource> classSource = findFirstClassSource(child);
            if (classSource.isPresent()) {
                return classSource;
            }
        }
        return Optional.empty();
    }

    /*
    A "store" for sysout/syserr content that gets sent to the AbstractJUnitResultFormatter.
    This store first uses a relatively decent sized in-memory buffer for storing the sysout/syserr
    content. This in-memory buffer will be used as long as it can fit in the new content that
    keeps coming in. When the size limit is reached, this store switches to a file based store
    by creating a temporarily file and writing out the already in-memory held buffer content
    and any new content that keeps arriving to this store. Once the file has been created,
    the in-memory buffer will never be used any more and in fact is destroyed as soon as the
    file is created.
    Instances of this class are not thread-safe and users of this class are expected to use necessary thread
    safety guarantees, if they want to use an instance of this class by multiple threads.
    */
    private static final class SysOutErrContentStore implements Closeable {
        private static final int DEFAULT_CAPACITY_IN_BYTES = 50 * 1024; // 50 KB
        private static final Reader EMPTY_READER = new Reader() {
            @Override
            public int read(final char[] cbuf, final int off, final int len) throws IOException {
                return -1;
            }

            @Override
            public void close() throws IOException {
            }
        };

        private final TestExecutionContext context;
        private final String tmpFileSuffix;
        private ByteBuffer inMemoryStore = ByteBuffer.allocate(DEFAULT_CAPACITY_IN_BYTES);
        private boolean usingFileStore = false;
        private Path filePath;
        private OutputStream fileOutputStream;

        private SysOutErrContentStore(final TestExecutionContext context, final boolean isSysOut) {
            this.context = context;
            this.tmpFileSuffix = isSysOut ? ".sysout" : ".syserr";
        }

        private void store(final byte[] data) throws IOException {
            if (this.usingFileStore) {
                this.storeToFile(data, 0, data.length);
                return;
            }
            // we haven't yet created a file store and the data can fit in memory,
            // so we write it in our buffer
            try {
                this.inMemoryStore.put(data);
                return;
            } catch (BufferOverflowException boe) {
                // the buffer capacity can't hold this incoming data, so this
                // incoming data hasn't been transferred to the buffer. let's
                // now fall back to a file store
                this.usingFileStore = true;
            }
            // since the content couldn't be transferred into in-memory buffer,
            // we now create a file and transfer already (previously) stored in-memory
            // content into that file, before finally transferring this new content
            // into the file too. We then finally discard this in-memory buffer and
            // just keep using the file store instead
            this.fileOutputStream = createFileStore();
            // first the existing in-memory content
            storeToFile(this.inMemoryStore.array(), 0, this.inMemoryStore.position());
            storeToFile(data, 0, data.length);
            // discard the in-memory store
            this.inMemoryStore = null;
        }

        private void storeToFile(final byte[] data, final int offset, final int length) throws IOException {
            if (this.fileOutputStream == null) {
                // no backing file was created so we can't do anything
                return;
            }
            this.fileOutputStream.write(data, offset, length);
        }

        private OutputStream createFileStore() throws IOException {
            this.filePath = FileUtils.getFileUtils()
                                     .createTempFile(context.getProject().orElse(null), null, this.tmpFileSuffix, null, true, true)
                                     .toPath();
            return Files.newOutputStream(this.filePath);
        }

        /*
         * Returns a Reader for reading the sysout/syserr content. If there's no data
         * available in this store, then this returns a Reader which when used for read operations,
         * will immediately indicate an EOF.
         */
        private Reader getReader() throws IOException {
            if (this.usingFileStore && this.filePath != null) {
                // we use a FileReader here so that we can use the system default character
                // encoding for reading the contents on sysout/syserr stream, since that's the
                // encoding that System.out/System.err uses to write out the messages
                return new BufferedReader(new FileReader(this.filePath.toFile()));
            }
            if (this.inMemoryStore != null) {
                return new InputStreamReader(new ByteArrayInputStream(this.inMemoryStore.array(), 0, this.inMemoryStore.position()));
            }
            // no data to read, so we return an "empty" reader
            return EMPTY_READER;
        }

        /*
         *  Returns true if this store has any data (either in-memory or in a file). Else
         *  returns false.
         */
        private boolean hasData() {
            if (this.inMemoryStore != null && this.inMemoryStore.position() > 0) {
                return true;
            }
            return this.usingFileStore && this.filePath != null;
        }

        @Override
        public void close() throws IOException {
            this.inMemoryStore = null;
            FileUtils.close(this.fileOutputStream);
            FileUtils.delete(this.filePath.toFile());
        }
    }
}
