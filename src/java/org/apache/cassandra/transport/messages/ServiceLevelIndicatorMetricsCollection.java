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
import org.apache.cassandra.metrics.ServiceLevelIndicatorMetrics;
import org.apache.cassandra.transport.ServerError;

// We are not speifically collecting metrics for the following exceptions: PROTOCOL_ERROR (0x000A),
// BAD_CREDENTIALS (0x0100) and all 2xx errors, because those errors can only be user errors.
public class ServiceLevelIndicatorMetricsCollection
{
    public static void collectMetrics(Exception ex) {
        if (ex instanceof RequestExecutionException)
        {
            switch (((RequestExecutionException) ex).code())
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
                    ServiceLevelIndicatorMetrics.otherExceptionMetrics.mark();
                    break;
            }
        } else if (ex instanceof ServerError) {
            ServiceLevelIndicatorMetrics.serverErrorExceptionMetrics.mark();
        } else {
            ServiceLevelIndicatorMetrics.otherExceptionMetrics.mark();
        }
    }
}
