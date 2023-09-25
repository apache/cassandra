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

package org.apache.cassandra.transport.messages;

import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.metrics.TransportMetrics;
import org.apache.cassandra.transport.ServerError;

// We are not speifically collecting metrics for the following exceptions: PROTOCOL_ERROR (0x000A),
// BAD_CREDENTIALS (0x0100) and all 2xx errors, because those errors can only be user errors.
public class ExceptionMetricsCollection
{
    public static void collectMetrics(Exception ex) {
        if (ex instanceof RequestExecutionException)
        {
            switch (((RequestExecutionException) ex).code())
            {
                case CDC_WRITE_FAILURE:
                    TransportMetrics.cdcWriteFailureExceptionCount.inc();
                    break;

                case WRITE_TIMEOUT:
                    TransportMetrics.writeTimeoutExceptionCount.inc();
                    break;

                case CAS_WRITE_UNKNOWN:
                    TransportMetrics.casWriteUnknownExceptionCount.inc();
                    break;

                case FUNCTION_FAILURE:
                    TransportMetrics.functionFailureExceptionCount.inc();
                    break;

                case IS_BOOTSTRAPPING:
                    TransportMetrics.isBootstrappingExceptionCount.inc();
                    break;

                case OVERLOADED:
                    TransportMetrics.overloadedExceptionCount.inc();
                    break;

                case READ_FAILURE:
                    TransportMetrics.readFailureExceptionCount.inc();
                    break;

                case READ_TIMEOUT:
                    TransportMetrics.readTimeoutExceptionCount.inc();
                    break;

                case TRUNCATE_ERROR:
                    TransportMetrics.truncateErrorExceptionCount.inc();
                    break;

                case UNAVAILABLE:
                    TransportMetrics.unavailableExceptionCount.inc();
                    break;

                case WRITE_FAILURE:
                    TransportMetrics.writeFailureExceptionCount.inc();
                    break;

                default:
                    TransportMetrics.otherExceptionCount.inc();
                    break;
            }
        } else if (ex instanceof ServerError) {
            TransportMetrics.serverErrorExceptionCount.inc();
        } else {
            TransportMetrics.otherExceptionCount.inc();
        }
    }
}
