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

package org.apache.cassandra.transport;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PasswordObfuscator;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.metrics.ServiceLevelIndicatorMetrics;
import org.apache.cassandra.utils.NoSpamLogger;

// Metrics for all CQL exceptions are collected.
public class ServiceLevelIndicatorMetricsCollection
{
    private static Logger logger = LoggerFactory.getLogger(ServiceLevelIndicatorMetricsCollection.class);
    public static void setLogger(Logger newLogger)
    {
        logger = newLogger;
    }

    public static void collectMetricsAndLog(Exception ex) {
        collectMetricsAndLog(ex, null);
    }

    public static void collectMetricsAndLog(Exception ex, String query) {
        ExceptionCode code = null;
        if (ex instanceof RequestExecutionException)
        {
            code = ((RequestExecutionException) ex).code();
            switch (code)
            {
                case CDC_WRITE_FAILURE:
                    ServiceLevelIndicatorMetrics.cdcWriteFailureExceptionMetrics.mark();
                    break;

                case WRITE_TIMEOUT:
                    ServiceLevelIndicatorMetrics.writeTimeoutExceptionMetrics.mark();
                    break;

                case CAS_WRITE_UNKNOWN:
                    ServiceLevelIndicatorMetrics.casWriteUnknownExceptionMetrics.mark();
                    break;

                case FUNCTION_FAILURE:
                    ServiceLevelIndicatorMetrics.functionFailureExceptionMetrics.mark();
                    break;

                case IS_BOOTSTRAPPING:
                    ServiceLevelIndicatorMetrics.isBootstrappingExceptionMetrics.mark();
                    break;

                case OVERLOADED:
                    ServiceLevelIndicatorMetrics.overloadedExceptionMetrics.mark();
                    break;

                case READ_FAILURE:
                    ServiceLevelIndicatorMetrics.readFailureExceptionMetrics.mark();
                    break;

                case READ_TIMEOUT:
                    ServiceLevelIndicatorMetrics.readTimeoutExceptionMetrics.mark();
                    break;

                case TRUNCATE_ERROR:
                    ServiceLevelIndicatorMetrics.truncateErrorExceptionMetrics.mark();
                    break;

                case UNAVAILABLE:
                    ServiceLevelIndicatorMetrics.unavailableExceptionMetrics.mark();
                    break;

                case WRITE_FAILURE:
                    ServiceLevelIndicatorMetrics.writeFailureExceptionMetrics.mark();
                    break;

                default:
                    code = null;
                    ServiceLevelIndicatorMetrics.otherExceptionMetrics.mark();
                    break;
            }
        }
        else if (ex instanceof RequestValidationException)
        {
            code = ((RequestValidationException) ex).code();
            switch (code)
            {
                case BAD_CREDENTIALS:
                    ServiceLevelIndicatorMetrics.badCredentialsExceptionMetrics.mark();
                    break;

                case SYNTAX_ERROR:
                    ServiceLevelIndicatorMetrics.syntaxErrorExceptionMetrics.mark();
                    break;

                case UNAUTHORIZED:
                    ServiceLevelIndicatorMetrics.unauthorizedExceptionMetrics.mark();
                    break;

                case INVALID:
                    ServiceLevelIndicatorMetrics.invalidExceptionMetrics.mark();
                    break;

                case CONFIG_ERROR:
                    ServiceLevelIndicatorMetrics.configErrorExceptionMetrics.mark();
                    break;

                case ALREADY_EXISTS:
                    ServiceLevelIndicatorMetrics.alreadyExistsExceptionMetrics.mark();
                    break;

                case UNPREPARED:
                    ServiceLevelIndicatorMetrics.unpreparedExceptionMetrics.mark();
                    break;

                default:
                    code = null;
                    ServiceLevelIndicatorMetrics.otherExceptionMetrics.mark();
                    break;
            }
        }
        else if (ex instanceof ServerError)
        {
            ServiceLevelIndicatorMetrics.serverErrorExceptionMetrics.mark();
            code = ExceptionCode.SERVER_ERROR;
        }
        else if (ex instanceof ProtocolException)
        {
            ServiceLevelIndicatorMetrics.protocolErrorExceptionMetrics.mark();
            code = ExceptionCode.PROTOCOL_ERROR;
        }
        else {
            code = null;
            ServiceLevelIndicatorMetrics.otherExceptionMetrics.mark();
        }
        if (DatabaseDescriptor.getServiceLevelIndicatorErrorLogEnabled())
        {
            String obfuscated = PasswordObfuscator.obfuscate(query);
            query =  obfuscated == null ? "null" : obfuscated;
            NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, ex.getClass().getSimpleName(), 1,
                             TimeUnit.MINUTES, "Service level indicator exception {} while executing {}",
                             code == null ? "unexpected CQL exception" : code.toString(), query, ex);
        }
    }
}
