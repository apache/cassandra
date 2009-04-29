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
public class HttpStartLineParser
{
    private Callback callback_;

    public interface Callback
    {
        void onStartLine(String method, String path, String query, String version);
    };
    
    public HttpStartLineParser(Callback cb)
    {
        callback_ = cb;
    }
    
    private enum StartLineParseState
    {
        EATING_WHITESPACE,
        READING_METHOD,
        READING_PATH,
        READING_QUERY,
        DECODING_FIRST_CHAR,
        DECODING_SECOND_CHAR,
        READING_VERSION,
        CHECKING_EOL,
        TO_RESET
    }
    
    private StartLineParseState parseState_ = StartLineParseState.TO_RESET;
    private StartLineParseState nextState_;
    private StringBuilder httpMethod_ = new StringBuilder(32);
    private StringBuilder httpPath_ = new StringBuilder();
    private StringBuilder httpQuery_ = new StringBuilder(32);
    private StringBuilder httpVersion_ = new StringBuilder();
    
    // we will encode things of the form %{2 digit hex number} and this is a 
    // temporary holder for the leftmost digit's value as the second digit is
    // being read
    private int encodedValue_;
    // this is a pointer to one of httpMethod_, httpPath_, httpQuery_, 
    // httpVersion_ so that the encoded value can be appended to the correct 
    // buffer
    private StringBuilder encodeTo_;

    public void resetParserState() {
        httpMethod_.setLength(0);
        httpPath_.setLength(0);
        httpQuery_.setLength(0);
        httpVersion_.setLength(0);
        
        parseState_ = StartLineParseState.EATING_WHITESPACE;
        nextState_ = StartLineParseState.READING_METHOD;
    }
    
    private void finishLine_()
    {
        if (callback_ != null) 
        {
            callback_.onStartLine(
                    httpMethod_.toString(), 
                    httpPath_.toString(), 
                    httpQuery_.toString(), 
                    httpVersion_.toString()
                    );
        }
    }
    
    private static int decodeHex(int hex)
    {
        if (hex >= '0' && hex <= '9')
        {
            return hex-'0';
        }
        else if (hex >= 'a' && hex <= 'f')
        {
            return hex-'a'+10;
        }
        else if (hex >= 'A' && hex <= 'F')
        {
            return hex-'A'+10;
        }
        else
        {
            return 0;
        }
    }
    
    public boolean onMoreBytes(InputStream in) throws HttpParsingException, IOException
    {
        int got;

        if (parseState_ == StartLineParseState.TO_RESET)
        {
            resetParserState();
        }

        while (in.available() > 0)
        {
            in.mark(1);
            got = in.read();
			
            switch (parseState_)
            {
                case EATING_WHITESPACE:
                        switch (got)
                        {
                            case ' ':
                                        break;
                            default:
                                        in.reset();
                                        parseState_ = nextState_;
                                        break;
                        }
                        break;
                    
                case READING_METHOD:
                        switch (got)
                        {
                            case ' ':
                                    parseState_ = StartLineParseState.EATING_WHITESPACE;
                                    nextState_ = StartLineParseState.READING_PATH;
                                    break;
                            default:
                                    httpMethod_.append((char) got);
                                    break;
                        }
                        break;
                        
                case READING_PATH:
                        switch (got)
                        {
                            case '\r':
                                    parseState_ = StartLineParseState.CHECKING_EOL;
                                    break;
                            case '%':
                                    encodeTo_ = httpPath_;
                                    nextState_ = parseState_;
                                    parseState_ = StartLineParseState.DECODING_FIRST_CHAR;
                                    break;
                            case ' ':
                                    parseState_ = StartLineParseState.EATING_WHITESPACE;
                                    nextState_ = StartLineParseState.READING_VERSION;
                                    break;
                            case '?':
                                    parseState_ = StartLineParseState.READING_QUERY;
                                    break;
                            default:
                                    httpPath_.append((char) got);
                                    break;
                        }
                        break;
                            
                case READING_QUERY:
                        switch (got)
                        {
                            case '\r':
                                    parseState_ = StartLineParseState.CHECKING_EOL;
                                    break;
                            case '%':
                                    encodeTo_ = httpQuery_;
                                    nextState_ = parseState_;
                                    parseState_ = StartLineParseState.DECODING_FIRST_CHAR;
                                    break;
                            case ' ':
                                    parseState_ = StartLineParseState.EATING_WHITESPACE;
                                    nextState_ = StartLineParseState.READING_VERSION;
                                    break;
                            case '+':
                                    httpQuery_.append(' ');
                                    break;
                            default:
                                    httpQuery_.append((char) got);
                                    break;
                        }
                        break;
                            
                case DECODING_FIRST_CHAR:
                        encodedValue_ = decodeHex(got) * 16;
                        parseState_ = StartLineParseState.DECODING_SECOND_CHAR;
                        break;

                case DECODING_SECOND_CHAR:
                        encodeTo_.append((char) (decodeHex(got) + encodedValue_));
                        parseState_ = nextState_;
                        break;
                                
                case READING_VERSION:
                        switch (got)
                        {
                            case '\r':
                                    parseState_ = StartLineParseState.CHECKING_EOL;
                                    break;
                            default:
                                    httpVersion_.append((char) got);
                                    break;
                        }
                        break;
                        
                case CHECKING_EOL:
                        switch (got)
                        {
                            case '\n':
                                    finishLine_();
                                    parseState_ = StartLineParseState.TO_RESET;
                                    return true;
                            default:
                                    throw new HttpParsingException();
                        }
                        
                default:
                        throw new HttpParsingException();
            }
        }
        
        return false;
    }
    
    public boolean onMoreBytesNew(ByteBuffer buffer) throws HttpParsingException, IOException
    {
        int got;
        int limit = buffer.limit();
        int pos = buffer.position();
    	
        if (parseState_ == StartLineParseState.TO_RESET)
        {
            resetParserState();
        }

        while(pos < limit) 
        {
            switch(parseState_)
            {	
                case EATING_WHITESPACE:
                        while((char)buffer.get(pos) == ' ' && ++pos < limit);
                        if(pos < limit)
                            parseState_ = nextState_;
                        break;

                case READING_METHOD:
                        while(pos < limit && (got = buffer.get(pos)) != ' ') 
                        {
                            httpMethod_.append((char)got);
                            pos++;
                        }

                        if(pos < limit)
                        {
                            parseState_ = StartLineParseState.EATING_WHITESPACE;
                            nextState_ = StartLineParseState.READING_PATH;
                        }
                        break;

                case READING_PATH:
                        while(pos < limit && parseState_ == StartLineParseState.READING_PATH) 
                        {
                            got = buffer.get(pos++);

                            switch (got)
                            {
                                case '\r':
                                        parseState_ = StartLineParseState.CHECKING_EOL;
                                        break;
                                case '%':
                                        encodeTo_ = httpPath_;
                                        nextState_ = parseState_;
                                        parseState_ = StartLineParseState.DECODING_FIRST_CHAR;
                                        break;
                                case ' ':
                                        parseState_ = StartLineParseState.EATING_WHITESPACE;
                                        nextState_ = StartLineParseState.READING_VERSION;
                                        break;
                                case '?':
                                        parseState_ = StartLineParseState.READING_QUERY;
                                        break;
                                default:
                                        httpPath_.append((char) got);
                                        break;
                            }
                        }
                        break;

                case READING_QUERY:
                        while(pos < limit && parseState_ == StartLineParseState.READING_QUERY) 
                        {
                            got = buffer.get(pos++);

                            switch (got)
                            {
                                case '\r':
                                        parseState_ = StartLineParseState.CHECKING_EOL;
                                        break;
                                case '%':
                                        encodeTo_ = httpQuery_;
                                        nextState_ = parseState_;
                                        parseState_ = StartLineParseState.DECODING_FIRST_CHAR;
                                        break;
                                case ' ':
                                        parseState_ = StartLineParseState.EATING_WHITESPACE;
                                        nextState_ = StartLineParseState.READING_VERSION;
                                        break;
                                case '+':
                                        httpQuery_.append(' ');
                                        break;
                                default:
                                        httpQuery_.append((char) got);
                                        break;
                            }
                        }
                        break;

                case DECODING_FIRST_CHAR:
                        got = (int)buffer.get(pos++);
                        encodedValue_ = decodeHex(got) * 16;
                        parseState_ = StartLineParseState.DECODING_SECOND_CHAR;
                        break;

                case DECODING_SECOND_CHAR:
                        got = (int)buffer.get(pos++);
                        encodeTo_.append((char) (decodeHex(got) + encodedValue_));
                        parseState_ = nextState_;
                        break;

                case READING_VERSION:
                        while(pos < limit && (got = buffer.get(pos)) != '\r' )
                        {
                            httpVersion_.append((char)got);
                            pos++;
                        }
                        if(pos < limit) 
                        {
                            parseState_ = StartLineParseState.CHECKING_EOL;
                            pos++; // skipping '\r'
                        }
                        break;

                case CHECKING_EOL:
                        switch (buffer.get(pos++))
                        {
                            case '\n':
                                    finishLine_();
                                    parseState_ = StartLineParseState.TO_RESET;
                                    buffer.position(pos);
                                    return true;  //could have reached limit here
                            default:
                                    throw new HttpParsingException();
                        }
                        
                default:
                        throw new HttpParsingException();
            }
        } 

        buffer.position(pos);
        return false;
    }
}

