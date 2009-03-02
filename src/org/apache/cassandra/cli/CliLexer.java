// $ANTLR 3.0.1 /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g 2008-10-29 16:05:52

package org.apache.cassandra.cli;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class CliLexer extends Lexer {
    public static final int K_TABLES=32;
    public static final int NODE_EXIT=6;
    public static final int K_EXIT=22;
    public static final int K_GET=24;
    public static final int K_CONNECT=18;
    public static final int K_CONFIG=29;
    public static final int EOF=-1;
    public static final int Identifier=36;
    public static final int K_SET=25;
    public static final int K_DESCRIBE=33;
    public static final int NODE_SHOW_VERSION=11;
    public static final int NODE_CONNECT=4;
    public static final int SLASH=19;
    public static final int NODE_SHOW_TABLES=12;
    public static final int K_CLUSTER=27;
    public static final int K_SHOW=26;
    public static final int NODE_DESCRIBE_TABLE=5;
    public static final int K_TABLE=34;
    public static final int COMMENT=42;
    public static final int DOT=35;
    public static final int K_NAME=28;
    public static final int K_QUIT=21;
    public static final int NODE_SHOW_CONFIG_FILE=10;
    public static final int K_VERSION=31;
    public static final int K_FILE=30;
    public static final int SEMICOLON=17;
    public static final int Digit=40;
    public static final int T43=43;
    public static final int Tokens=47;
    public static final int T46=46;
    public static final int NODE_THRIFT_GET=13;
    public static final int T45=45;
    public static final int T44=44;
    public static final int StringLiteral=37;
    public static final int NODE_HELP=7;
    public static final int NODE_NO_OP=8;
    public static final int NODE_THRIFT_SET=14;
    public static final int NODE_ID_LIST=16;
    public static final int WS=41;
    public static final int K_THRIFT=23;
    public static final int K_HELP=20;
    public static final int IntegerLiteral=38;
    public static final int NODE_SHOW_CLUSTER_NAME=9;
    public static final int Letter=39;
    public static final int NODE_COLUMN_ACCESS=15;
    public CliLexer() {;} 
    public CliLexer(CharStream input) {
        super(input);
    }
    public String getGrammarFileName() { return "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g"; }

    // $ANTLR start T43
    public final void mT43() throws RecognitionException {
        try {
            int _type = T43;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:6:5: ( '?' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:6:7: '?'
            {
            match('?'); 

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end T43

    // $ANTLR start T44
    public final void mT44() throws RecognitionException {
        try {
            int _type = T44;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:7:5: ( '=' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:7:7: '='
            {
            match('='); 

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end T44

    // $ANTLR start T45
    public final void mT45() throws RecognitionException {
        try {
            int _type = T45;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:8:5: ( '[' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:8:7: '['
            {
            match('['); 

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end T45

    // $ANTLR start T46
    public final void mT46() throws RecognitionException {
        try {
            int _type = T46;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:9:5: ( ']' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:9:7: ']'
            {
            match(']'); 

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end T46

    // $ANTLR start K_CONFIG
    public final void mK_CONFIG() throws RecognitionException {
        try {
            int _type = K_CONFIG;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:166:9: ( 'CONFIG' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:166:15: 'CONFIG'
            {
            match("CONFIG"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_CONFIG

    // $ANTLR start K_CONNECT
    public final void mK_CONNECT() throws RecognitionException {
        try {
            int _type = K_CONNECT;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:167:10: ( 'CONNECT' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:167:15: 'CONNECT'
            {
            match("CONNECT"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_CONNECT

    // $ANTLR start K_CLUSTER
    public final void mK_CLUSTER() throws RecognitionException {
        try {
            int _type = K_CLUSTER;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:168:10: ( 'CLUSTER' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:168:15: 'CLUSTER'
            {
            match("CLUSTER"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_CLUSTER

    // $ANTLR start K_DESCRIBE
    public final void mK_DESCRIBE() throws RecognitionException {
        try {
            int _type = K_DESCRIBE;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:169:11: ( 'DESCRIBE' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:169:15: 'DESCRIBE'
            {
            match("DESCRIBE"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_DESCRIBE

    // $ANTLR start K_GET
    public final void mK_GET() throws RecognitionException {
        try {
            int _type = K_GET;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:170:6: ( 'GET' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:170:15: 'GET'
            {
            match("GET"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_GET

    // $ANTLR start K_HELP
    public final void mK_HELP() throws RecognitionException {
        try {
            int _type = K_HELP;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:171:7: ( 'HELP' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:171:15: 'HELP'
            {
            match("HELP"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_HELP

    // $ANTLR start K_EXIT
    public final void mK_EXIT() throws RecognitionException {
        try {
            int _type = K_EXIT;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:172:7: ( 'EXIT' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:172:15: 'EXIT'
            {
            match("EXIT"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_EXIT

    // $ANTLR start K_FILE
    public final void mK_FILE() throws RecognitionException {
        try {
            int _type = K_FILE;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:173:7: ( 'FILE' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:173:15: 'FILE'
            {
            match("FILE"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_FILE

    // $ANTLR start K_NAME
    public final void mK_NAME() throws RecognitionException {
        try {
            int _type = K_NAME;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:174:7: ( 'NAME' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:174:15: 'NAME'
            {
            match("NAME"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_NAME

    // $ANTLR start K_QUIT
    public final void mK_QUIT() throws RecognitionException {
        try {
            int _type = K_QUIT;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:175:7: ( 'QUIT' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:175:15: 'QUIT'
            {
            match("QUIT"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_QUIT

    // $ANTLR start K_SET
    public final void mK_SET() throws RecognitionException {
        try {
            int _type = K_SET;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:176:6: ( 'SET' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:176:15: 'SET'
            {
            match("SET"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_SET

    // $ANTLR start K_SHOW
    public final void mK_SHOW() throws RecognitionException {
        try {
            int _type = K_SHOW;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:177:7: ( 'SHOW' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:177:15: 'SHOW'
            {
            match("SHOW"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_SHOW

    // $ANTLR start K_TABLE
    public final void mK_TABLE() throws RecognitionException {
        try {
            int _type = K_TABLE;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:178:8: ( 'TABLE' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:178:15: 'TABLE'
            {
            match("TABLE"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_TABLE

    // $ANTLR start K_TABLES
    public final void mK_TABLES() throws RecognitionException {
        try {
            int _type = K_TABLES;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:179:9: ( 'TABLES' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:179:15: 'TABLES'
            {
            match("TABLES"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_TABLES

    // $ANTLR start K_THRIFT
    public final void mK_THRIFT() throws RecognitionException {
        try {
            int _type = K_THRIFT;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:180:9: ( 'THRIFT' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:180:15: 'THRIFT'
            {
            match("THRIFT"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_THRIFT

    // $ANTLR start K_VERSION
    public final void mK_VERSION() throws RecognitionException {
        try {
            int _type = K_VERSION;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:181:10: ( 'VERSION' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:181:15: 'VERSION'
            {
            match("VERSION"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_VERSION

    // $ANTLR start Letter
    public final void mLetter() throws RecognitionException {
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:186:5: ( 'a' .. 'z' | 'A' .. 'Z' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:
            {
            if ( (input.LA(1)>='A' && input.LA(1)<='Z')||(input.LA(1)>='a' && input.LA(1)<='z') ) {
                input.consume();

            }
            else {
                MismatchedSetException mse =
                    new MismatchedSetException(null,input);
                recover(mse);    throw mse;
            }


            }

        }
        finally {
        }
    }
    // $ANTLR end Letter

    // $ANTLR start Digit
    public final void mDigit() throws RecognitionException {
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:192:5: ( '0' .. '9' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:192:7: '0' .. '9'
            {
            matchRange('0','9'); 

            }

        }
        finally {
        }
    }
    // $ANTLR end Digit

    // $ANTLR start Identifier
    public final void mIdentifier() throws RecognitionException {
        try {
            int _type = Identifier;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:197:5: ( Letter ( Letter | Digit | '_' )* )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:197:7: Letter ( Letter | Digit | '_' )*
            {
            mLetter(); 
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:197:14: ( Letter | Digit | '_' )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0>='0' && LA1_0<='9')||(LA1_0>='A' && LA1_0<='Z')||LA1_0=='_'||(LA1_0>='a' && LA1_0<='z')) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:
            	    {
            	    if ( (input.LA(1)>='0' && input.LA(1)<='9')||(input.LA(1)>='A' && input.LA(1)<='Z')||input.LA(1)=='_'||(input.LA(1)>='a' && input.LA(1)<='z') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse =
            	            new MismatchedSetException(null,input);
            	        recover(mse);    throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    break loop1;
                }
            } while (true);


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end Identifier

    // $ANTLR start StringLiteral
    public final void mStringLiteral() throws RecognitionException {
        try {
            int _type = StringLiteral;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:203:5: ( '\\'' (~ '\\'' )* '\\'' ( '\\'' (~ '\\'' )* '\\'' )* )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:204:5: '\\'' (~ '\\'' )* '\\'' ( '\\'' (~ '\\'' )* '\\'' )*
            {
            match('\''); 
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:204:10: (~ '\\'' )*
            loop2:
            do {
                int alt2=2;
                int LA2_0 = input.LA(1);

                if ( ((LA2_0>='\u0000' && LA2_0<='&')||(LA2_0>='(' && LA2_0<='\uFFFE')) ) {
                    alt2=1;
                }


                switch (alt2) {
            	case 1 :
            	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:204:11: ~ '\\''
            	    {
            	    if ( (input.LA(1)>='\u0000' && input.LA(1)<='&')||(input.LA(1)>='(' && input.LA(1)<='\uFFFE') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse =
            	            new MismatchedSetException(null,input);
            	        recover(mse);    throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    break loop2;
                }
            } while (true);

            match('\''); 
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:204:24: ( '\\'' (~ '\\'' )* '\\'' )*
            loop4:
            do {
                int alt4=2;
                int LA4_0 = input.LA(1);

                if ( (LA4_0=='\'') ) {
                    alt4=1;
                }


                switch (alt4) {
            	case 1 :
            	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:204:26: '\\'' (~ '\\'' )* '\\''
            	    {
            	    match('\''); 
            	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:204:31: (~ '\\'' )*
            	    loop3:
            	    do {
            	        int alt3=2;
            	        int LA3_0 = input.LA(1);

            	        if ( ((LA3_0>='\u0000' && LA3_0<='&')||(LA3_0>='(' && LA3_0<='\uFFFE')) ) {
            	            alt3=1;
            	        }


            	        switch (alt3) {
            	    	case 1 :
            	    	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:204:32: ~ '\\''
            	    	    {
            	    	    if ( (input.LA(1)>='\u0000' && input.LA(1)<='&')||(input.LA(1)>='(' && input.LA(1)<='\uFFFE') ) {
            	    	        input.consume();

            	    	    }
            	    	    else {
            	    	        MismatchedSetException mse =
            	    	            new MismatchedSetException(null,input);
            	    	        recover(mse);    throw mse;
            	    	    }


            	    	    }
            	    	    break;

            	    	default :
            	    	    break loop3;
            	        }
            	    } while (true);

            	    match('\''); 

            	    }
            	    break;

            	default :
            	    break loop4;
                }
            } while (true);


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end StringLiteral

    // $ANTLR start IntegerLiteral
    public final void mIntegerLiteral() throws RecognitionException {
        try {
            int _type = IntegerLiteral;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:208:4: ( ( Digit )+ )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:208:6: ( Digit )+
            {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:208:6: ( Digit )+
            int cnt5=0;
            loop5:
            do {
                int alt5=2;
                int LA5_0 = input.LA(1);

                if ( ((LA5_0>='0' && LA5_0<='9')) ) {
                    alt5=1;
                }


                switch (alt5) {
            	case 1 :
            	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:208:6: Digit
            	    {
            	    mDigit(); 

            	    }
            	    break;

            	default :
            	    if ( cnt5 >= 1 ) break loop5;
                        EarlyExitException eee =
                            new EarlyExitException(5, input);
                        throw eee;
                }
                cnt5++;
            } while (true);


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end IntegerLiteral

    // $ANTLR start DOT
    public final void mDOT() throws RecognitionException {
        try {
            int _type = DOT;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:216:5: ( '.' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:216:7: '.'
            {
            match('.'); 

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end DOT

    // $ANTLR start SLASH
    public final void mSLASH() throws RecognitionException {
        try {
            int _type = SLASH;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:220:5: ( '/' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:220:7: '/'
            {
            match('/'); 

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end SLASH

    // $ANTLR start SEMICOLON
    public final void mSEMICOLON() throws RecognitionException {
        try {
            int _type = SEMICOLON;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:224:5: ( ';' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:224:7: ';'
            {
            match(';'); 

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end SEMICOLON

    // $ANTLR start WS
    public final void mWS() throws RecognitionException {
        try {
            int _type = WS;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:228:5: ( ( ' ' | '\\r' | '\\t' | '\\n' ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:228:8: ( ' ' | '\\r' | '\\t' | '\\n' )
            {
            if ( (input.LA(1)>='\t' && input.LA(1)<='\n')||input.LA(1)=='\r'||input.LA(1)==' ' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse =
                    new MismatchedSetException(null,input);
                recover(mse);    throw mse;
            }

            channel=HIDDEN;

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end WS

    // $ANTLR start COMMENT
    public final void mCOMMENT() throws RecognitionException {
        try {
            int _type = COMMENT;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:232:5: ( '--' (~ ( '\\n' | '\\r' ) )* | '/*' ( options {greedy=false; } : . )* '*/' )
            int alt8=2;
            int LA8_0 = input.LA(1);

            if ( (LA8_0=='-') ) {
                alt8=1;
            }
            else if ( (LA8_0=='/') ) {
                alt8=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("231:1: COMMENT : ( '--' (~ ( '\\n' | '\\r' ) )* | '/*' ( options {greedy=false; } : . )* '*/' );", 8, 0, input);

                throw nvae;
            }
            switch (alt8) {
                case 1 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:232:7: '--' (~ ( '\\n' | '\\r' ) )*
                    {
                    match("--"); 

                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:232:12: (~ ( '\\n' | '\\r' ) )*
                    loop6:
                    do {
                        int alt6=2;
                        int LA6_0 = input.LA(1);

                        if ( ((LA6_0>='\u0000' && LA6_0<='\t')||(LA6_0>='\u000B' && LA6_0<='\f')||(LA6_0>='\u000E' && LA6_0<='\uFFFE')) ) {
                            alt6=1;
                        }


                        switch (alt6) {
                    	case 1 :
                    	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:232:13: ~ ( '\\n' | '\\r' )
                    	    {
                    	    if ( (input.LA(1)>='\u0000' && input.LA(1)<='\t')||(input.LA(1)>='\u000B' && input.LA(1)<='\f')||(input.LA(1)>='\u000E' && input.LA(1)<='\uFFFE') ) {
                    	        input.consume();

                    	    }
                    	    else {
                    	        MismatchedSetException mse =
                    	            new MismatchedSetException(null,input);
                    	        recover(mse);    throw mse;
                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop6;
                        }
                    } while (true);

                     channel=HIDDEN; 

                    }
                    break;
                case 2 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:233:7: '/*' ( options {greedy=false; } : . )* '*/'
                    {
                    match("/*"); 

                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:233:12: ( options {greedy=false; } : . )*
                    loop7:
                    do {
                        int alt7=2;
                        int LA7_0 = input.LA(1);

                        if ( (LA7_0=='*') ) {
                            int LA7_1 = input.LA(2);

                            if ( (LA7_1=='/') ) {
                                alt7=2;
                            }
                            else if ( ((LA7_1>='\u0000' && LA7_1<='.')||(LA7_1>='0' && LA7_1<='\uFFFE')) ) {
                                alt7=1;
                            }


                        }
                        else if ( ((LA7_0>='\u0000' && LA7_0<=')')||(LA7_0>='+' && LA7_0<='\uFFFE')) ) {
                            alt7=1;
                        }


                        switch (alt7) {
                    	case 1 :
                    	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:233:39: .
                    	    {
                    	    matchAny(); 

                    	    }
                    	    break;

                    	default :
                    	    break loop7;
                        }
                    } while (true);

                    match("*/"); 

                     channel=HIDDEN; 

                    }
                    break;

            }
            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end COMMENT

    public void mTokens() throws RecognitionException {
        // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:8: ( T43 | T44 | T45 | T46 | K_CONFIG | K_CONNECT | K_CLUSTER | K_DESCRIBE | K_GET | K_HELP | K_EXIT | K_FILE | K_NAME | K_QUIT | K_SET | K_SHOW | K_TABLE | K_TABLES | K_THRIFT | K_VERSION | Identifier | StringLiteral | IntegerLiteral | DOT | SLASH | SEMICOLON | WS | COMMENT )
        int alt9=28;
        switch ( input.LA(1) ) {
        case '?':
            {
            alt9=1;
            }
            break;
        case '=':
            {
            alt9=2;
            }
            break;
        case '[':
            {
            alt9=3;
            }
            break;
        case ']':
            {
            alt9=4;
            }
            break;
        case 'C':
            {
            switch ( input.LA(2) ) {
            case 'O':
                {
                int LA9_24 = input.LA(3);

                if ( (LA9_24=='N') ) {
                    switch ( input.LA(4) ) {
                    case 'N':
                        {
                        int LA9_53 = input.LA(5);

                        if ( (LA9_53=='E') ) {
                            int LA9_68 = input.LA(6);

                            if ( (LA9_68=='C') ) {
                                int LA9_81 = input.LA(7);

                                if ( (LA9_81=='T') ) {
                                    int LA9_89 = input.LA(8);

                                    if ( ((LA9_89>='0' && LA9_89<='9')||(LA9_89>='A' && LA9_89<='Z')||LA9_89=='_'||(LA9_89>='a' && LA9_89<='z')) ) {
                                        alt9=21;
                                    }
                                    else {
                                        alt9=6;}
                                }
                                else {
                                    alt9=21;}
                            }
                            else {
                                alt9=21;}
                        }
                        else {
                            alt9=21;}
                        }
                        break;
                    case 'F':
                        {
                        int LA9_54 = input.LA(5);

                        if ( (LA9_54=='I') ) {
                            int LA9_69 = input.LA(6);

                            if ( (LA9_69=='G') ) {
                                int LA9_82 = input.LA(7);

                                if ( ((LA9_82>='0' && LA9_82<='9')||(LA9_82>='A' && LA9_82<='Z')||LA9_82=='_'||(LA9_82>='a' && LA9_82<='z')) ) {
                                    alt9=21;
                                }
                                else {
                                    alt9=5;}
                            }
                            else {
                                alt9=21;}
                        }
                        else {
                            alt9=21;}
                        }
                        break;
                    default:
                        alt9=21;}

                }
                else {
                    alt9=21;}
                }
                break;
            case 'L':
                {
                int LA9_25 = input.LA(3);

                if ( (LA9_25=='U') ) {
                    int LA9_40 = input.LA(4);

                    if ( (LA9_40=='S') ) {
                        int LA9_55 = input.LA(5);

                        if ( (LA9_55=='T') ) {
                            int LA9_70 = input.LA(6);

                            if ( (LA9_70=='E') ) {
                                int LA9_83 = input.LA(7);

                                if ( (LA9_83=='R') ) {
                                    int LA9_91 = input.LA(8);

                                    if ( ((LA9_91>='0' && LA9_91<='9')||(LA9_91>='A' && LA9_91<='Z')||LA9_91=='_'||(LA9_91>='a' && LA9_91<='z')) ) {
                                        alt9=21;
                                    }
                                    else {
                                        alt9=7;}
                                }
                                else {
                                    alt9=21;}
                            }
                            else {
                                alt9=21;}
                        }
                        else {
                            alt9=21;}
                    }
                    else {
                        alt9=21;}
                }
                else {
                    alt9=21;}
                }
                break;
            default:
                alt9=21;}

            }
            break;
        case 'D':
            {
            int LA9_6 = input.LA(2);

            if ( (LA9_6=='E') ) {
                int LA9_26 = input.LA(3);

                if ( (LA9_26=='S') ) {
                    int LA9_41 = input.LA(4);

                    if ( (LA9_41=='C') ) {
                        int LA9_56 = input.LA(5);

                        if ( (LA9_56=='R') ) {
                            int LA9_71 = input.LA(6);

                            if ( (LA9_71=='I') ) {
                                int LA9_84 = input.LA(7);

                                if ( (LA9_84=='B') ) {
                                    int LA9_92 = input.LA(8);

                                    if ( (LA9_92=='E') ) {
                                        int LA9_98 = input.LA(9);

                                        if ( ((LA9_98>='0' && LA9_98<='9')||(LA9_98>='A' && LA9_98<='Z')||LA9_98=='_'||(LA9_98>='a' && LA9_98<='z')) ) {
                                            alt9=21;
                                        }
                                        else {
                                            alt9=8;}
                                    }
                                    else {
                                        alt9=21;}
                                }
                                else {
                                    alt9=21;}
                            }
                            else {
                                alt9=21;}
                        }
                        else {
                            alt9=21;}
                    }
                    else {
                        alt9=21;}
                }
                else {
                    alt9=21;}
            }
            else {
                alt9=21;}
            }
            break;
        case 'G':
            {
            int LA9_7 = input.LA(2);

            if ( (LA9_7=='E') ) {
                int LA9_27 = input.LA(3);

                if ( (LA9_27=='T') ) {
                    int LA9_42 = input.LA(4);

                    if ( ((LA9_42>='0' && LA9_42<='9')||(LA9_42>='A' && LA9_42<='Z')||LA9_42=='_'||(LA9_42>='a' && LA9_42<='z')) ) {
                        alt9=21;
                    }
                    else {
                        alt9=9;}
                }
                else {
                    alt9=21;}
            }
            else {
                alt9=21;}
            }
            break;
        case 'H':
            {
            int LA9_8 = input.LA(2);

            if ( (LA9_8=='E') ) {
                int LA9_28 = input.LA(3);

                if ( (LA9_28=='L') ) {
                    int LA9_43 = input.LA(4);

                    if ( (LA9_43=='P') ) {
                        int LA9_58 = input.LA(5);

                        if ( ((LA9_58>='0' && LA9_58<='9')||(LA9_58>='A' && LA9_58<='Z')||LA9_58=='_'||(LA9_58>='a' && LA9_58<='z')) ) {
                            alt9=21;
                        }
                        else {
                            alt9=10;}
                    }
                    else {
                        alt9=21;}
                }
                else {
                    alt9=21;}
            }
            else {
                alt9=21;}
            }
            break;
        case 'E':
            {
            int LA9_9 = input.LA(2);

            if ( (LA9_9=='X') ) {
                int LA9_29 = input.LA(3);

                if ( (LA9_29=='I') ) {
                    int LA9_44 = input.LA(4);

                    if ( (LA9_44=='T') ) {
                        int LA9_59 = input.LA(5);

                        if ( ((LA9_59>='0' && LA9_59<='9')||(LA9_59>='A' && LA9_59<='Z')||LA9_59=='_'||(LA9_59>='a' && LA9_59<='z')) ) {
                            alt9=21;
                        }
                        else {
                            alt9=11;}
                    }
                    else {
                        alt9=21;}
                }
                else {
                    alt9=21;}
            }
            else {
                alt9=21;}
            }
            break;
        case 'F':
            {
            int LA9_10 = input.LA(2);

            if ( (LA9_10=='I') ) {
                int LA9_30 = input.LA(3);

                if ( (LA9_30=='L') ) {
                    int LA9_45 = input.LA(4);

                    if ( (LA9_45=='E') ) {
                        int LA9_60 = input.LA(5);

                        if ( ((LA9_60>='0' && LA9_60<='9')||(LA9_60>='A' && LA9_60<='Z')||LA9_60=='_'||(LA9_60>='a' && LA9_60<='z')) ) {
                            alt9=21;
                        }
                        else {
                            alt9=12;}
                    }
                    else {
                        alt9=21;}
                }
                else {
                    alt9=21;}
            }
            else {
                alt9=21;}
            }
            break;
        case 'N':
            {
            int LA9_11 = input.LA(2);

            if ( (LA9_11=='A') ) {
                int LA9_31 = input.LA(3);

                if ( (LA9_31=='M') ) {
                    int LA9_46 = input.LA(4);

                    if ( (LA9_46=='E') ) {
                        int LA9_61 = input.LA(5);

                        if ( ((LA9_61>='0' && LA9_61<='9')||(LA9_61>='A' && LA9_61<='Z')||LA9_61=='_'||(LA9_61>='a' && LA9_61<='z')) ) {
                            alt9=21;
                        }
                        else {
                            alt9=13;}
                    }
                    else {
                        alt9=21;}
                }
                else {
                    alt9=21;}
            }
            else {
                alt9=21;}
            }
            break;
        case 'Q':
            {
            int LA9_12 = input.LA(2);

            if ( (LA9_12=='U') ) {
                int LA9_32 = input.LA(3);

                if ( (LA9_32=='I') ) {
                    int LA9_47 = input.LA(4);

                    if ( (LA9_47=='T') ) {
                        int LA9_62 = input.LA(5);

                        if ( ((LA9_62>='0' && LA9_62<='9')||(LA9_62>='A' && LA9_62<='Z')||LA9_62=='_'||(LA9_62>='a' && LA9_62<='z')) ) {
                            alt9=21;
                        }
                        else {
                            alt9=14;}
                    }
                    else {
                        alt9=21;}
                }
                else {
                    alt9=21;}
            }
            else {
                alt9=21;}
            }
            break;
        case 'S':
            {
            switch ( input.LA(2) ) {
            case 'H':
                {
                int LA9_33 = input.LA(3);

                if ( (LA9_33=='O') ) {
                    int LA9_48 = input.LA(4);

                    if ( (LA9_48=='W') ) {
                        int LA9_63 = input.LA(5);

                        if ( ((LA9_63>='0' && LA9_63<='9')||(LA9_63>='A' && LA9_63<='Z')||LA9_63=='_'||(LA9_63>='a' && LA9_63<='z')) ) {
                            alt9=21;
                        }
                        else {
                            alt9=16;}
                    }
                    else {
                        alt9=21;}
                }
                else {
                    alt9=21;}
                }
                break;
            case 'E':
                {
                int LA9_34 = input.LA(3);

                if ( (LA9_34=='T') ) {
                    int LA9_49 = input.LA(4);

                    if ( ((LA9_49>='0' && LA9_49<='9')||(LA9_49>='A' && LA9_49<='Z')||LA9_49=='_'||(LA9_49>='a' && LA9_49<='z')) ) {
                        alt9=21;
                    }
                    else {
                        alt9=15;}
                }
                else {
                    alt9=21;}
                }
                break;
            default:
                alt9=21;}

            }
            break;
        case 'T':
            {
            switch ( input.LA(2) ) {
            case 'A':
                {
                int LA9_35 = input.LA(3);

                if ( (LA9_35=='B') ) {
                    int LA9_50 = input.LA(4);

                    if ( (LA9_50=='L') ) {
                        int LA9_65 = input.LA(5);

                        if ( (LA9_65=='E') ) {
                            switch ( input.LA(6) ) {
                            case 'S':
                                {
                                int LA9_85 = input.LA(7);

                                if ( ((LA9_85>='0' && LA9_85<='9')||(LA9_85>='A' && LA9_85<='Z')||LA9_85=='_'||(LA9_85>='a' && LA9_85<='z')) ) {
                                    alt9=21;
                                }
                                else {
                                    alt9=18;}
                                }
                                break;
                            case '0':
                            case '1':
                            case '2':
                            case '3':
                            case '4':
                            case '5':
                            case '6':
                            case '7':
                            case '8':
                            case '9':
                            case 'A':
                            case 'B':
                            case 'C':
                            case 'D':
                            case 'E':
                            case 'F':
                            case 'G':
                            case 'H':
                            case 'I':
                            case 'J':
                            case 'K':
                            case 'L':
                            case 'M':
                            case 'N':
                            case 'O':
                            case 'P':
                            case 'Q':
                            case 'R':
                            case 'T':
                            case 'U':
                            case 'V':
                            case 'W':
                            case 'X':
                            case 'Y':
                            case 'Z':
                            case '_':
                            case 'a':
                            case 'b':
                            case 'c':
                            case 'd':
                            case 'e':
                            case 'f':
                            case 'g':
                            case 'h':
                            case 'i':
                            case 'j':
                            case 'k':
                            case 'l':
                            case 'm':
                            case 'n':
                            case 'o':
                            case 'p':
                            case 'q':
                            case 'r':
                            case 's':
                            case 't':
                            case 'u':
                            case 'v':
                            case 'w':
                            case 'x':
                            case 'y':
                            case 'z':
                                {
                                alt9=21;
                                }
                                break;
                            default:
                                alt9=17;}

                        }
                        else {
                            alt9=21;}
                    }
                    else {
                        alt9=21;}
                }
                else {
                    alt9=21;}
                }
                break;
            case 'H':
                {
                int LA9_36 = input.LA(3);

                if ( (LA9_36=='R') ) {
                    int LA9_51 = input.LA(4);

                    if ( (LA9_51=='I') ) {
                        int LA9_66 = input.LA(5);

                        if ( (LA9_66=='F') ) {
                            int LA9_79 = input.LA(6);

                            if ( (LA9_79=='T') ) {
                                int LA9_87 = input.LA(7);

                                if ( ((LA9_87>='0' && LA9_87<='9')||(LA9_87>='A' && LA9_87<='Z')||LA9_87=='_'||(LA9_87>='a' && LA9_87<='z')) ) {
                                    alt9=21;
                                }
                                else {
                                    alt9=19;}
                            }
                            else {
                                alt9=21;}
                        }
                        else {
                            alt9=21;}
                    }
                    else {
                        alt9=21;}
                }
                else {
                    alt9=21;}
                }
                break;
            default:
                alt9=21;}

            }
            break;
        case 'V':
            {
            int LA9_15 = input.LA(2);

            if ( (LA9_15=='E') ) {
                int LA9_37 = input.LA(3);

                if ( (LA9_37=='R') ) {
                    int LA9_52 = input.LA(4);

                    if ( (LA9_52=='S') ) {
                        int LA9_67 = input.LA(5);

                        if ( (LA9_67=='I') ) {
                            int LA9_80 = input.LA(6);

                            if ( (LA9_80=='O') ) {
                                int LA9_88 = input.LA(7);

                                if ( (LA9_88=='N') ) {
                                    int LA9_95 = input.LA(8);

                                    if ( ((LA9_95>='0' && LA9_95<='9')||(LA9_95>='A' && LA9_95<='Z')||LA9_95=='_'||(LA9_95>='a' && LA9_95<='z')) ) {
                                        alt9=21;
                                    }
                                    else {
                                        alt9=20;}
                                }
                                else {
                                    alt9=21;}
                            }
                            else {
                                alt9=21;}
                        }
                        else {
                            alt9=21;}
                    }
                    else {
                        alt9=21;}
                }
                else {
                    alt9=21;}
            }
            else {
                alt9=21;}
            }
            break;
        case 'A':
        case 'B':
        case 'I':
        case 'J':
        case 'K':
        case 'L':
        case 'M':
        case 'O':
        case 'P':
        case 'R':
        case 'U':
        case 'W':
        case 'X':
        case 'Y':
        case 'Z':
        case 'a':
        case 'b':
        case 'c':
        case 'd':
        case 'e':
        case 'f':
        case 'g':
        case 'h':
        case 'i':
        case 'j':
        case 'k':
        case 'l':
        case 'm':
        case 'n':
        case 'o':
        case 'p':
        case 'q':
        case 'r':
        case 's':
        case 't':
        case 'u':
        case 'v':
        case 'w':
        case 'x':
        case 'y':
        case 'z':
            {
            alt9=21;
            }
            break;
        case '\'':
            {
            alt9=22;
            }
            break;
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            {
            alt9=23;
            }
            break;
        case '.':
            {
            alt9=24;
            }
            break;
        case '/':
            {
            int LA9_20 = input.LA(2);

            if ( (LA9_20=='*') ) {
                alt9=28;
            }
            else {
                alt9=25;}
            }
            break;
        case ';':
            {
            alt9=26;
            }
            break;
        case '\t':
        case '\n':
        case '\r':
        case ' ':
            {
            alt9=27;
            }
            break;
        case '-':
            {
            alt9=28;
            }
            break;
        default:
            NoViableAltException nvae =
                new NoViableAltException("1:1: Tokens : ( T43 | T44 | T45 | T46 | K_CONFIG | K_CONNECT | K_CLUSTER | K_DESCRIBE | K_GET | K_HELP | K_EXIT | K_FILE | K_NAME | K_QUIT | K_SET | K_SHOW | K_TABLE | K_TABLES | K_THRIFT | K_VERSION | Identifier | StringLiteral | IntegerLiteral | DOT | SLASH | SEMICOLON | WS | COMMENT );", 9, 0, input);

            throw nvae;
        }

        switch (alt9) {
            case 1 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:10: T43
                {
                mT43(); 

                }
                break;
            case 2 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:14: T44
                {
                mT44(); 

                }
                break;
            case 3 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:18: T45
                {
                mT45(); 

                }
                break;
            case 4 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:22: T46
                {
                mT46(); 

                }
                break;
            case 5 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:26: K_CONFIG
                {
                mK_CONFIG(); 

                }
                break;
            case 6 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:35: K_CONNECT
                {
                mK_CONNECT(); 

                }
                break;
            case 7 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:45: K_CLUSTER
                {
                mK_CLUSTER(); 

                }
                break;
            case 8 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:55: K_DESCRIBE
                {
                mK_DESCRIBE(); 

                }
                break;
            case 9 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:66: K_GET
                {
                mK_GET(); 

                }
                break;
            case 10 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:72: K_HELP
                {
                mK_HELP(); 

                }
                break;
            case 11 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:79: K_EXIT
                {
                mK_EXIT(); 

                }
                break;
            case 12 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:86: K_FILE
                {
                mK_FILE(); 

                }
                break;
            case 13 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:93: K_NAME
                {
                mK_NAME(); 

                }
                break;
            case 14 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:100: K_QUIT
                {
                mK_QUIT(); 

                }
                break;
            case 15 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:107: K_SET
                {
                mK_SET(); 

                }
                break;
            case 16 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:113: K_SHOW
                {
                mK_SHOW(); 

                }
                break;
            case 17 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:120: K_TABLE
                {
                mK_TABLE(); 

                }
                break;
            case 18 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:128: K_TABLES
                {
                mK_TABLES(); 

                }
                break;
            case 19 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:137: K_THRIFT
                {
                mK_THRIFT(); 

                }
                break;
            case 20 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:146: K_VERSION
                {
                mK_VERSION(); 

                }
                break;
            case 21 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:156: Identifier
                {
                mIdentifier(); 

                }
                break;
            case 22 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:167: StringLiteral
                {
                mStringLiteral(); 

                }
                break;
            case 23 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:181: IntegerLiteral
                {
                mIntegerLiteral(); 

                }
                break;
            case 24 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:196: DOT
                {
                mDOT(); 

                }
                break;
            case 25 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:200: SLASH
                {
                mSLASH(); 

                }
                break;
            case 26 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:206: SEMICOLON
                {
                mSEMICOLON(); 

                }
                break;
            case 27 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:216: WS
                {
                mWS(); 

                }
                break;
            case 28 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:1:219: COMMENT
                {
                mCOMMENT(); 

                }
                break;

        }

    }


 

}