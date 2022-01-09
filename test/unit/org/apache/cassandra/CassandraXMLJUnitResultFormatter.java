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

import java.io.IOException;
import java.io.Reader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.junit.platform.launcher.TestIdentifier;

import com.sun.xml.internal.txw2.output.IndentingXMLStreamWriter;
import org.apache.cassandra.junitlauncher.LegacyXmlResultFormatter;
import org.apache.tools.ant.util.DOMElementWriter;
import org.apache.tools.ant.util.StringUtils;

/**
 * Prints XML output of the test to a specified Writer.
 */
public class CassandraXMLJUnitResultFormatter extends LegacyXmlResultFormatter
{
    private static final String tag = System.getProperty("cassandra.testtag", "");

    @Override
    protected String determineTestName(TestIdentifier testId)
    {
        String testName = super.determineTestName(testId);
        if (!tag.isEmpty())
            testName = testName + '-' + tag;
        return testName;
    }

    @Override
    protected XMLReportWriter createXMLReportWriter()
    {
        return new CassandraXMLReportWriter();
    }

    class CassandraXMLReportWriter extends XMLReportWriter
    {
        private static final String ELEM_SYSTEM_OUT = "system-out";
        private static final String ELEM_SYSTEM_ERR = "system-err";
        private static final String ELEM_HOSTNAME = "hostname";

        private static final String ELEM_FAILURE = "failure";
        private static final String ATTR_MESSAGE = "message";
        private static final String ATTR_TYPE = "type";

        @Override
        protected XMLStreamWriter createXMLStreamWriter() throws XMLStreamException
        {
            return new IndentingXMLStreamWriter(super.createXMLStreamWriter());
        }

        @Override
        protected void writeCustomAttributes(XMLStreamWriter writer) throws XMLStreamException
        {
            writer.writeAttribute(ELEM_HOSTNAME, getHostName());
        }

        @Override
        protected void writeSysOut(XMLStreamWriter writer) throws XMLStreamException, IOException
        {
            writer.writeStartElement(ELEM_SYSTEM_OUT);
            if (!hasSysOut())
                writer.writeCData("");
            else
                try (final Reader reader = getSysOutReader())
                {
                    writeCDataFrom(reader, writer);
                }
            writer.writeEndElement();
        }

        @Override
        protected void writeSysErr(XMLStreamWriter writer) throws XMLStreamException, IOException
        {
            writer.writeStartElement(ELEM_SYSTEM_ERR);
            if (!hasSysErr())
                writer.writeCData("");
            else
                try (final Reader reader = getSysErrReader())
                {
                    writeCDataFrom(reader, writer);
                }
            writer.writeEndElement();
        }

        @Override
        protected void writeFailed(final XMLStreamWriter writer, final TestIdentifier testIdentifier) throws XMLStreamException
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

        private void writeCDataFrom(final Reader reader, final XMLStreamWriter writer) throws IOException, XMLStreamException
        {
            final char[] chars = new char[1024];
            int numRead;
            while ((numRead = reader.read(chars)) != -1)
            {
                writer.writeCData(encode(new String(chars, 0, numRead)));
            }
        }

        @Override
        protected String encode(String s)
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

        @Override
        protected String determineTestSuiteName()
        {
            String testSuiteName = super.determineTestSuiteName();
            if (!"UNKNOWN".equals(testSuiteName) && !tag.isEmpty())
                testSuiteName = testSuiteName + '-' + tag;
            return testSuiteName;
        }

        /**
         * Get the local hostname.
         *
         * @return the name of the local host, or "localhost" if we cannot work it out
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
