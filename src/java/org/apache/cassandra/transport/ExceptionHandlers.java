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

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.unix.Errors;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Throwables;

public class ExceptionHandlers
{
    private static final Logger logger = LoggerFactory.getLogger(ExceptionHandlers.class);

    public static ChannelInboundHandlerAdapter postV5Handler(FrameEncoder.PayloadAllocator allocator,
                                                             ProtocolVersion version)
    {
        return new PostV5ExceptionHandler(allocator, version);
    }

    private static final class PostV5ExceptionHandler extends ChannelInboundHandlerAdapter
    {
        private final FrameEncoder.PayloadAllocator allocator;
        private final ProtocolVersion version;

        public PostV5ExceptionHandler(FrameEncoder.PayloadAllocator allocator, ProtocolVersion version)
        {
            this.allocator = allocator;
            this.version = version;
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, Throwable cause)
        {
            // Provide error message to client in case channel is still open
            if (ctx.channel().isOpen())
            {
                Predicate<Throwable> handler = getUnexpectedExceptionHandler(ctx.channel(), false);
                ErrorMessage errorMessage = ErrorMessage.fromException(cause, handler);
                Envelope response = errorMessage.encode(version);
                FrameEncoder.Payload payload = allocator.allocate(true, CQLMessageHandler.envelopeSize(response.header));
                try
                {
                    response.encodeInto(payload.buffer);
                    response.release();
                    payload.finish();
                    ChannelPromise promise = ctx.newPromise();
                    // On protocol exception, close the channel as soon as the message has been sent
                    if (isFatal(cause))
                        promise.addListener(future -> ctx.close());
                    ctx.writeAndFlush(payload, promise);
                }
                finally
                {
                    payload.release();
                    JVMStabilityInspector.inspectThrowable(cause);
                }
            }
            
            if (DatabaseDescriptor.getClientErrorReportingExclusions().contains(ctx.channel().remoteAddress()))
            {
                // Sometimes it is desirable to ignore exceptions from specific IPs; such as when security scans are
                // running.  To avoid polluting logs and metrics, metrics are not updated when the IP is in the exclude
                // list.
                logger.debug("Excluding client exception for {}; address contained in client_error_reporting_exclusions", ctx.channel().remoteAddress(), cause);
                return;
            }
            logClientNetworkingExceptions(cause);
        }

        private static boolean isFatal(Throwable cause)
        {
            return Throwables.anyCauseMatches(cause, t -> t instanceof ProtocolException
                                                          && ((ProtocolException)t).isFatal());
        }
    }

    static void logClientNetworkingExceptions(Throwable cause)
    {
        if (Throwables.anyCauseMatches(cause, t -> t instanceof ProtocolException))
        {
            // if any ProtocolExceptions is not silent, then handle
            if (Throwables.anyCauseMatches(cause, t -> t instanceof ProtocolException && !((ProtocolException) t).isSilent()))
            {
                ClientMetrics.instance.markProtocolException();
                // since protocol exceptions are expected to be client issues, not logging stack trace
                // to avoid spamming the logs once a bad client shows up
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES, "Protocol exception with client networking: " + cause.getMessage());
            }
        }
        else if (Throwables.anyCauseMatches(cause, t -> t instanceof OverloadedException))
        {
            // Once the threshold for overload is breached, it will very likely spam the logs...
            NoSpamLogger.log(logger, NoSpamLogger.Level.INFO, 1, TimeUnit.MINUTES, cause.getMessage());
        }
        else if (Throwables.anyCauseMatches(cause, t -> t instanceof Errors.NativeIoException))
        {
            ClientMetrics.instance.markUnknownException();
            logger.trace("Native exception in client networking", cause);
        }
        else
        {
            ClientMetrics.instance.markUnknownException();
            logger.warn("Unknown exception in client networking", cause);
        }
    }

    static Predicate<Throwable> getUnexpectedExceptionHandler(Channel channel, boolean alwaysLogAtError)
    {
        SocketAddress address = channel.remoteAddress();
        if (DatabaseDescriptor.getClientErrorReportingExclusions().contains(address))
        {
            return cause -> {
                logger.debug("Excluding client exception for {}; address contained in client_error_reporting_exclusions", address, cause);
                return true;
            };
        }
        return new UnexpectedChannelExceptionHandler(channel, alwaysLogAtError);
    }

    /**
     * Include the channel info in the logged information for unexpected errors, and (if {@link #alwaysLogAtError} is
     * false then choose the log level based on the type of exception (some are clearly client issues and shouldn't be
     * logged at server ERROR level)
     */
    static final class UnexpectedChannelExceptionHandler implements Predicate<Throwable>
    {

        /**
         * When we encounter an unexpected IOException we look for these {@link Throwable#getMessage() messages}
         * (because we have no better way to distinguish) and log them at DEBUG rather than INFO, since they
         * are generally caused by unclean client disconnects rather than an actual problem.
         */
        private static final Set<String> ioExceptionsAtDebugLevel = ImmutableSet.<String>builder().
            add("Connection reset by peer").
            add("Broken pipe").
            add("Connection timed out").
            build();

        private final Channel channel;
        private final boolean alwaysLogAtError;

        UnexpectedChannelExceptionHandler(Channel channel, boolean alwaysLogAtError)
        {
            this.channel = channel;
            this.alwaysLogAtError = alwaysLogAtError;
        }

        @Override
        public boolean apply(Throwable exception)
        {
            String message;
            try
            {
                message = "Unexpected exception during request; channel = " + channel;
            }
            catch (Exception ignore)
            {
                // We don't want to make things worse if String.valueOf() throws an exception
                message = "Unexpected exception during request; channel = <unprintable>";
            }

            // netty wraps SSL errors in a CodecExcpetion
            if (!alwaysLogAtError && (exception instanceof IOException || (exception.getCause() instanceof IOException)))
            {
                String errorMessage = exception.getMessage();
                boolean logAtTrace = false;

                for (String ioException : ioExceptionsAtDebugLevel)
                {
                    // exceptions thrown from the netty epoll transport add the name of the function that failed
                    // to the exception string (which is simply wrapping a JDK exception), so we can't do a simple/naive comparison
                    if (errorMessage.contains(ioException))
                    {
                        logAtTrace = true;
                        break;
                    }
                }

                if (logAtTrace)
                {
                    // Likely unclean client disconnects
                    logger.trace(message, exception);
                }
                else
                {
                    // Generally unhandled IO exceptions are network issues, not actual ERRORS
                    logger.info(message, exception);
                }
            }
            else
            {
                // Anything else is probably a bug in server of client binary protocol handling
                logger.error(message, exception);
            }

            // We handled the exception.
            return true;
        }
    }
}
