/**
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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.cassandra.net.http;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 *
 * @author kranganathan
 */
/**
 * A parser for HTTP header lines.  
 * 
 */
public class HttpHeaderParser
{

    private Callback callback_;

    public interface Callback
    {

        public void onHeader(String key, String value);
    }

    public HttpHeaderParser(Callback cb)
    {
        callback_ = cb;
    }

    enum HeaderParseState
    {
        // we are at the very beginning of the line
        START_OF_HEADER_LINE,
        // are at line beginning, read '\r' but ran out of bytes in this round
        START_OF_HEADER_LINE_WITH_READ_SLASH_R,
        // we are in the process of parsing a header key
        IN_HEADER_KEY,
        // eat whitespace after the ':' but before the value
        PRE_HEADER_VALUE_WHITESPACE,
        // we are in the process of parsing a header value
        IN_HEADER_VALUE,
        // were in IN_HEADER_VALUE and read '\r' but ran out of more bytes
        IN_HEADER_VALUE_WITH_READ_SLASH_R,
        /*
         * got \r\n in the header value.  now consider whether its a multilined 
         * value.  For example,
         *
         * HeaderKey: HeaderValue\r\n this is still part of the value\r\n
         * 
         * is a valid HTTP header line with value 
         *
         * HeaderValue\r\n this is still part of the value
         *
         * NOTE: while all whitespace should generally be condensed into a 
         * single space by the HTTP standard, we will just preserve all of the
         * whitespace for now
         * 
         * TODO: consider replacing all whitespace with a single space
         * 
         * TODO: this parser doesn't correctly preserve the \r\n, should it?
         */
        CHECKING_END_OF_VALUE,
        // we are just about to reset the state of the header parser
        TO_RESET
    }

    // the current state of the parser
    private HeaderParseState parseState_ = HeaderParseState.TO_RESET;
    // incrementally build up this HTTP header key as we read it
    private StringBuilder headerKey_ = new StringBuilder(32);

    // incrementally build up this HTTP header value as we read it
    private StringBuilder headerValue_ = new StringBuilder(64);

    public void resetParserState()
    {
        headerKey_.setLength(0);
        headerValue_.setLength(0);
        parseState_ = HeaderParseState.START_OF_HEADER_LINE;
    }

    private void finishCurrentHeader_()
    {
        if (callback_ != null)
        {
            callback_.onHeader(headerKey_.toString().trim(), headerValue_
                    .toString().trim());
        }
        resetParserState();
    }

    public boolean onMoreBytes(InputStream in) throws IOException
    {
        int got;

        if (parseState_ == HeaderParseState.TO_RESET)
        {
            resetParserState();
        }

        while (in.available() > 0)
        {
            in.mark(1);
            got = in.read();

            switch (parseState_)
            {

            case START_OF_HEADER_LINE:
                switch (got)
                {
                case '\r':
                    if (in.available() > 0)
                    {
                        in.mark(1);
                        got = in.read();

                        if (got == '\n')
                        {
                            parseState_ = HeaderParseState.TO_RESET;
                            return true;
                        } // TODO: determine whether this \r-eating is valid
                        else
                        {
                            in.reset();
                        }
                    } // wait for more data to make this decision
                    else
                    {
                        in.reset();
                        return false;
                    }
                    break;

                default:
                    in.reset();
                    parseState_ = HeaderParseState.IN_HEADER_KEY;
                    break;
                }
                break;

            case IN_HEADER_KEY:
                switch (got)
                {
                case ':':
                    parseState_ = HeaderParseState.PRE_HEADER_VALUE_WHITESPACE;
                    break;
                // TODO: find out: whether to eat whitespace before a : 
                default:
                    headerKey_.append((char) got);
                    break;
                }
                break;

            case PRE_HEADER_VALUE_WHITESPACE:
                switch (got)
                {
                case ' ':
                case '\t':
                    break;
                default:
                    in.reset();
                    parseState_ = HeaderParseState.IN_HEADER_VALUE;
                    break;
                }
                break;

            case IN_HEADER_VALUE:
                switch (got)
                {
                case '\r':
                    if (in.available() > 0)
                    {
                        in.mark(1);
                        got = in.read();

                        if (got == '\n')
                        {
                            parseState_ = HeaderParseState.CHECKING_END_OF_VALUE;
                            break;
                        } // TODO: determine whether this \r-eating is valid
                        else
                        {
                            in.reset();
                        }
                    }
                    else
                    {
                        in.reset();
                        return false;
                    }
                    break;
                default:
                    headerValue_.append((char) got);
                    break;
                }
                break;

            case CHECKING_END_OF_VALUE:
                switch (got)
                {
                case ' ':
                case '\t':
                    in.reset();
                    parseState_ = HeaderParseState.IN_HEADER_VALUE;
                    break;
                default:
                    in.reset();
                    finishCurrentHeader_();
                }
                break;
            default:
                assert false;
                parseState_ = HeaderParseState.START_OF_HEADER_LINE;
                break;
            }
        }

        return false;
    }

    public boolean onMoreBytesNew(ByteBuffer buffer) throws IOException
    {

        int got;
        int limit = buffer.limit();
        int pos = buffer.position();

        if (parseState_ == HeaderParseState.TO_RESET)
        {
            resetParserState();
        }

        while (pos < limit)
        {
            switch (parseState_)
            {

            case START_OF_HEADER_LINE:
                if ((got = buffer.get(pos)) != '\r')
                {
                    parseState_ = HeaderParseState.IN_HEADER_KEY;
                    break;
                }
                else
                {
                    pos++;
                    if (pos == limit) // Need more bytes
                    {
                        buffer.position(pos);
                        parseState_ = HeaderParseState.START_OF_HEADER_LINE_WITH_READ_SLASH_R;
                        return false;
                    }
                }
            // fall through

            case START_OF_HEADER_LINE_WITH_READ_SLASH_R:
                // Processed "...\r\n\r\n" - headers are complete
                if (((char) buffer.get(pos)) == '\n')
                {
                    buffer.position(++pos);
                    parseState_ = HeaderParseState.TO_RESET;
                    return true;
                } // TODO: determine whether this \r-eating is valid
                else
                {
                    parseState_ = HeaderParseState.IN_HEADER_KEY;
                }
            //fall through

            case IN_HEADER_KEY:
                // TODO: find out: whether to eat whitespace before a :
                while (pos < limit && (got = buffer.get(pos)) != ':')
                {
                    headerKey_.append((char) got);
                    pos++;
                }
                if (pos < limit)
                {
                    pos++; //eating ':'
                    parseState_ = HeaderParseState.PRE_HEADER_VALUE_WHITESPACE;
                }
                break;

            case PRE_HEADER_VALUE_WHITESPACE:
                while ((((got = buffer.get(pos)) == ' ') || (got == '\t'))
                        && (++pos < limit))
                {
                    ;
                }
                if (pos < limit)
                {
                    parseState_ = HeaderParseState.IN_HEADER_VALUE;
                }
                break;

            case IN_HEADER_VALUE:
                while (pos < limit && (got = buffer.get(pos)) != '\r')
                {
                    headerValue_.append((char) got);
                    pos++;
                }
                if (pos == limit)
                {
                    break;
                }

                pos++;
                if (pos == limit)
                {
                    parseState_ = HeaderParseState.IN_HEADER_VALUE_WITH_READ_SLASH_R;
                    break;
                    //buffer.position(pos);
                    //return false;
                }
            // fall through

            case IN_HEADER_VALUE_WITH_READ_SLASH_R:
                if (((char) buffer.get(pos)) == '\n')
                {
                    parseState_ = HeaderParseState.CHECKING_END_OF_VALUE;
                    pos++;
                } // TODO: determine whether this \r-eating is valid
                else
                {
                    parseState_ = HeaderParseState.IN_HEADER_VALUE;
                }
                break;

            case CHECKING_END_OF_VALUE:
                switch ((char) buffer.get(pos))
                {
                case ' ':
                case '\t':
                    parseState_ = HeaderParseState.IN_HEADER_VALUE;
                    break;

                default:
                    // Processed "headerKey headerValue\r\n"
                    finishCurrentHeader_();
                }
                break;

            default:
                assert false;
                parseState_ = HeaderParseState.START_OF_HEADER_LINE;
                break;
            }

        }
        // Need to read more bytes - get next buffer
        buffer.position(pos);
        return false;
    }
}
