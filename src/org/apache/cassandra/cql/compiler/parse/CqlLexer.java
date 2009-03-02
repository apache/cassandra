// $ANTLR 3.0.1 /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g 2008-10-24 16:15:03

            package org.apache.cassandra.cql.compiler.parse;
        

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class CqlLexer extends Lexer {
    public static final int K_EXPLAIN=23;
    public static final int K_OFFSET=41;
    public static final int K_GET=25;
    public static final int K_DELETE=33;
    public static final int A_KEY_EXACT_MATCH=14;
    public static final int K_BY=40;
    public static final int A_SELECT=6;
    public static final int A_SUPERCOLUMN_MAP_VALUE=19;
    public static final int EOF=-1;
    public static final int K_SELECT=27;
    public static final int K_LIMIT=31;
    public static final int Identifier=34;
    public static final int K_SET=26;
    public static final int K_WHERE=29;
    public static final int COMMA=36;
    public static final int A_EXPLAIN_PLAN=8;
    public static final int A_LIMIT=15;
    public static final int COMMENT=46;
    public static final int K_ORDER=42;
    public static final int RIGHT_BRACE=37;
    public static final int A_COLUMN_MAP_VALUE=11;
    public static final int T49=49;
    public static final int T48=48;
    public static final int SEMICOLON=22;
    public static final int K_IN=30;
    public static final int Digit=44;
    public static final int Tokens=54;
    public static final int A_OFFSET=16;
    public static final int A_WHERE=21;
    public static final int K_PLAN=24;
    public static final int T47=47;
    public static final int A_ORDER_BY=17;
    public static final int K_FROM=28;
    public static final int StringLiteral=39;
    public static final int A_COLUMN_MAP_ENTRY=10;
    public static final int WS=45;
    public static final int T50=50;
    public static final int A_FROM=12;
    public static final int A_GET=5;
    public static final int LEFT_BRACE=35;
    public static final int A_KEY_IN_LIST=13;
    public static final int A_COLUMN_ACCESS=9;
    public static final int A_SUPERCOLUMN_MAP_ENTRY=18;
    public static final int IntegerLiteral=32;
    public static final int ASSOC=38;
    public static final int T52=52;
    public static final int T51=51;
    public static final int A_SELECT_CLAUSE=20;
    public static final int T53=53;
    public static final int Letter=43;
    public static final int A_DELETE=4;
    public static final int A_SET=7;
    public CqlLexer() {;} 
    public CqlLexer(CharStream input) {
        super(input);
    }
    public String getGrammarFileName() { return "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g"; }

    // $ANTLR start T47
    public final void mT47() throws RecognitionException {
        try {
            int _type = T47;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:6:5: ( '=' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:6:7: '='
            {
            match('='); 

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end T47

    // $ANTLR start T48
    public final void mT48() throws RecognitionException {
        try {
            int _type = T48;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:7:5: ( '(' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:7:7: '('
            {
            match('('); 

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end T48

    // $ANTLR start T49
    public final void mT49() throws RecognitionException {
        try {
            int _type = T49;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:8:5: ( ')' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:8:7: ')'
            {
            match(')'); 

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end T49

    // $ANTLR start T50
    public final void mT50() throws RecognitionException {
        try {
            int _type = T50;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:9:5: ( '[' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:9:7: '['
            {
            match('['); 

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end T50

    // $ANTLR start T51
    public final void mT51() throws RecognitionException {
        try {
            int _type = T51;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:10:5: ( ']' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:10:7: ']'
            {
            match(']'); 

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end T51

    // $ANTLR start T52
    public final void mT52() throws RecognitionException {
        try {
            int _type = T52;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:11:5: ( '.' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:11:7: '.'
            {
            match('.'); 

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end T52

    // $ANTLR start T53
    public final void mT53() throws RecognitionException {
        try {
            int _type = T53;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:12:5: ( '?' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:12:7: '?'
            {
            match('?'); 

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end T53

    // $ANTLR start K_BY
    public final void mK_BY() throws RecognitionException {
        try {
            int _type = K_BY;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:247:5: ( 'BY' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:247:14: 'BY'
            {
            match("BY"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_BY

    // $ANTLR start K_DELETE
    public final void mK_DELETE() throws RecognitionException {
        try {
            int _type = K_DELETE;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:248:9: ( 'DELETE' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:248:14: 'DELETE'
            {
            match("DELETE"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_DELETE

    // $ANTLR start K_EXPLAIN
    public final void mK_EXPLAIN() throws RecognitionException {
        try {
            int _type = K_EXPLAIN;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:249:10: ( 'EXPLAIN' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:249:14: 'EXPLAIN'
            {
            match("EXPLAIN"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_EXPLAIN

    // $ANTLR start K_FROM
    public final void mK_FROM() throws RecognitionException {
        try {
            int _type = K_FROM;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:250:7: ( 'FROM' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:250:14: 'FROM'
            {
            match("FROM"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_FROM

    // $ANTLR start K_GET
    public final void mK_GET() throws RecognitionException {
        try {
            int _type = K_GET;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:251:6: ( 'GET' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:251:14: 'GET'
            {
            match("GET"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_GET

    // $ANTLR start K_IN
    public final void mK_IN() throws RecognitionException {
        try {
            int _type = K_IN;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:252:5: ( 'IN' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:252:14: 'IN'
            {
            match("IN"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_IN

    // $ANTLR start K_LIMIT
    public final void mK_LIMIT() throws RecognitionException {
        try {
            int _type = K_LIMIT;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:253:8: ( 'LIMIT' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:253:14: 'LIMIT'
            {
            match("LIMIT"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_LIMIT

    // $ANTLR start K_OFFSET
    public final void mK_OFFSET() throws RecognitionException {
        try {
            int _type = K_OFFSET;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:254:9: ( 'OFFSET' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:254:14: 'OFFSET'
            {
            match("OFFSET"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_OFFSET

    // $ANTLR start K_ORDER
    public final void mK_ORDER() throws RecognitionException {
        try {
            int _type = K_ORDER;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:255:8: ( 'ORDER' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:255:14: 'ORDER'
            {
            match("ORDER"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_ORDER

    // $ANTLR start K_PLAN
    public final void mK_PLAN() throws RecognitionException {
        try {
            int _type = K_PLAN;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:256:7: ( 'PLAN' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:256:14: 'PLAN'
            {
            match("PLAN"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_PLAN

    // $ANTLR start K_SELECT
    public final void mK_SELECT() throws RecognitionException {
        try {
            int _type = K_SELECT;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:257:9: ( 'SELECT' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:257:14: 'SELECT'
            {
            match("SELECT"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_SELECT

    // $ANTLR start K_SET
    public final void mK_SET() throws RecognitionException {
        try {
            int _type = K_SET;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:258:6: ( 'SET' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:258:14: 'SET'
            {
            match("SET"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_SET

    // $ANTLR start K_WHERE
    public final void mK_WHERE() throws RecognitionException {
        try {
            int _type = K_WHERE;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:259:8: ( 'WHERE' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:259:14: 'WHERE'
            {
            match("WHERE"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end K_WHERE

    // $ANTLR start Letter
    public final void mLetter() throws RecognitionException {
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:264:5: ( 'a' .. 'z' | 'A' .. 'Z' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:
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
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:270:5: ( '0' .. '9' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:270:7: '0' .. '9'
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
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:275:5: ( Letter ( Letter | Digit | '_' )* )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:275:7: Letter ( Letter | Digit | '_' )*
            {
            mLetter(); 
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:275:14: ( Letter | Digit | '_' )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0>='0' && LA1_0<='9')||(LA1_0>='A' && LA1_0<='Z')||LA1_0=='_'||(LA1_0>='a' && LA1_0<='z')) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:
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
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:284:5: ( '\\'' (~ '\\'' )* '\\'' ( '\\'' (~ '\\'' )* '\\'' )* )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:284:7: '\\'' (~ '\\'' )* '\\'' ( '\\'' (~ '\\'' )* '\\'' )*
            {
            match('\''); 
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:284:12: (~ '\\'' )*
            loop2:
            do {
                int alt2=2;
                int LA2_0 = input.LA(1);

                if ( ((LA2_0>='\u0000' && LA2_0<='&')||(LA2_0>='(' && LA2_0<='\uFFFE')) ) {
                    alt2=1;
                }


                switch (alt2) {
            	case 1 :
            	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:284:13: ~ '\\''
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
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:284:26: ( '\\'' (~ '\\'' )* '\\'' )*
            loop4:
            do {
                int alt4=2;
                int LA4_0 = input.LA(1);

                if ( (LA4_0=='\'') ) {
                    alt4=1;
                }


                switch (alt4) {
            	case 1 :
            	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:284:28: '\\'' (~ '\\'' )* '\\''
            	    {
            	    match('\''); 
            	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:284:33: (~ '\\'' )*
            	    loop3:
            	    do {
            	        int alt3=2;
            	        int LA3_0 = input.LA(1);

            	        if ( ((LA3_0>='\u0000' && LA3_0<='&')||(LA3_0>='(' && LA3_0<='\uFFFE')) ) {
            	            alt3=1;
            	        }


            	        switch (alt3) {
            	    	case 1 :
            	    	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:284:34: ~ '\\''
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
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:289:5: ( ( Digit )+ )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:289:7: ( Digit )+
            {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:289:7: ( Digit )+
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
            	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:289:7: Digit
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

    // $ANTLR start WS
    public final void mWS() throws RecognitionException {
        try {
            int _type = WS;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:296:5: ( ( ' ' | '\\r' | '\\t' | '\\n' ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:296:8: ( ' ' | '\\r' | '\\t' | '\\n' )
            {
            if ( (input.LA(1)>='\t' && input.LA(1)<='\n')||input.LA(1)=='\r'||input.LA(1)==' ' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse =
                    new MismatchedSetException(null,input);
                recover(mse);    throw mse;
            }

            skip();

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
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:300:5: ( '--' (~ ( '\\n' | '\\r' ) )* | '/*' ( options {greedy=false; } : . )* '*/' )
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
                    new NoViableAltException("299:1: COMMENT : ( '--' (~ ( '\\n' | '\\r' ) )* | '/*' ( options {greedy=false; } : . )* '*/' );", 8, 0, input);

                throw nvae;
            }
            switch (alt8) {
                case 1 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:300:7: '--' (~ ( '\\n' | '\\r' ) )*
                    {
                    match("--"); 

                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:300:12: (~ ( '\\n' | '\\r' ) )*
                    loop6:
                    do {
                        int alt6=2;
                        int LA6_0 = input.LA(1);

                        if ( ((LA6_0>='\u0000' && LA6_0<='\t')||(LA6_0>='\u000B' && LA6_0<='\f')||(LA6_0>='\u000E' && LA6_0<='\uFFFE')) ) {
                            alt6=1;
                        }


                        switch (alt6) {
                    	case 1 :
                    	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:300:13: ~ ( '\\n' | '\\r' )
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
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:301:7: '/*' ( options {greedy=false; } : . )* '*/'
                    {
                    match("/*"); 

                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:301:12: ( options {greedy=false; } : . )*
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
                    	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:301:39: .
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

    // $ANTLR start ASSOC
    public final void mASSOC() throws RecognitionException {
        try {
            int _type = ASSOC;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:304:6: ( '=>' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:304:15: '=>'
            {
            match("=>"); 


            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end ASSOC

    // $ANTLR start COMMA
    public final void mCOMMA() throws RecognitionException {
        try {
            int _type = COMMA;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:305:6: ( ',' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:305:15: ','
            {
            match(','); 

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end COMMA

    // $ANTLR start LEFT_BRACE
    public final void mLEFT_BRACE() throws RecognitionException {
        try {
            int _type = LEFT_BRACE;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:306:11: ( '{' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:306:15: '{'
            {
            match('{'); 

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end LEFT_BRACE

    // $ANTLR start RIGHT_BRACE
    public final void mRIGHT_BRACE() throws RecognitionException {
        try {
            int _type = RIGHT_BRACE;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:307:12: ( '}' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:307:15: '}'
            {
            match('}'); 

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end RIGHT_BRACE

    // $ANTLR start SEMICOLON
    public final void mSEMICOLON() throws RecognitionException {
        try {
            int _type = SEMICOLON;
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:308:10: ( ';' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:308:15: ';'
            {
            match(';'); 

            }

            this.type = _type;
        }
        finally {
        }
    }
    // $ANTLR end SEMICOLON

    public void mTokens() throws RecognitionException {
        // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:8: ( T47 | T48 | T49 | T50 | T51 | T52 | T53 | K_BY | K_DELETE | K_EXPLAIN | K_FROM | K_GET | K_IN | K_LIMIT | K_OFFSET | K_ORDER | K_PLAN | K_SELECT | K_SET | K_WHERE | Identifier | StringLiteral | IntegerLiteral | WS | COMMENT | ASSOC | COMMA | LEFT_BRACE | RIGHT_BRACE | SEMICOLON )
        int alt9=30;
        switch ( input.LA(1) ) {
        case '=':
            {
            int LA9_1 = input.LA(2);

            if ( (LA9_1=='>') ) {
                alt9=26;
            }
            else {
                alt9=1;}
            }
            break;
        case '(':
            {
            alt9=2;
            }
            break;
        case ')':
            {
            alt9=3;
            }
            break;
        case '[':
            {
            alt9=4;
            }
            break;
        case ']':
            {
            alt9=5;
            }
            break;
        case '.':
            {
            alt9=6;
            }
            break;
        case '?':
            {
            alt9=7;
            }
            break;
        case 'B':
            {
            int LA9_8 = input.LA(2);

            if ( (LA9_8=='Y') ) {
                int LA9_30 = input.LA(3);

                if ( ((LA9_30>='0' && LA9_30<='9')||(LA9_30>='A' && LA9_30<='Z')||LA9_30=='_'||(LA9_30>='a' && LA9_30<='z')) ) {
                    alt9=21;
                }
                else {
                    alt9=8;}
            }
            else {
                alt9=21;}
            }
            break;
        case 'D':
            {
            int LA9_9 = input.LA(2);

            if ( (LA9_9=='E') ) {
                int LA9_31 = input.LA(3);

                if ( (LA9_31=='L') ) {
                    int LA9_43 = input.LA(4);

                    if ( (LA9_43=='E') ) {
                        int LA9_55 = input.LA(5);

                        if ( (LA9_55=='T') ) {
                            int LA9_66 = input.LA(6);

                            if ( (LA9_66=='E') ) {
                                int LA9_75 = input.LA(7);

                                if ( ((LA9_75>='0' && LA9_75<='9')||(LA9_75>='A' && LA9_75<='Z')||LA9_75=='_'||(LA9_75>='a' && LA9_75<='z')) ) {
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
            int LA9_10 = input.LA(2);

            if ( (LA9_10=='X') ) {
                int LA9_32 = input.LA(3);

                if ( (LA9_32=='P') ) {
                    int LA9_44 = input.LA(4);

                    if ( (LA9_44=='L') ) {
                        int LA9_56 = input.LA(5);

                        if ( (LA9_56=='A') ) {
                            int LA9_67 = input.LA(6);

                            if ( (LA9_67=='I') ) {
                                int LA9_76 = input.LA(7);

                                if ( (LA9_76=='N') ) {
                                    int LA9_83 = input.LA(8);

                                    if ( ((LA9_83>='0' && LA9_83<='9')||(LA9_83>='A' && LA9_83<='Z')||LA9_83=='_'||(LA9_83>='a' && LA9_83<='z')) ) {
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
            int LA9_11 = input.LA(2);

            if ( (LA9_11=='R') ) {
                int LA9_33 = input.LA(3);

                if ( (LA9_33=='O') ) {
                    int LA9_45 = input.LA(4);

                    if ( (LA9_45=='M') ) {
                        int LA9_57 = input.LA(5);

                        if ( ((LA9_57>='0' && LA9_57<='9')||(LA9_57>='A' && LA9_57<='Z')||LA9_57=='_'||(LA9_57>='a' && LA9_57<='z')) ) {
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
        case 'G':
            {
            int LA9_12 = input.LA(2);

            if ( (LA9_12=='E') ) {
                int LA9_34 = input.LA(3);

                if ( (LA9_34=='T') ) {
                    int LA9_46 = input.LA(4);

                    if ( ((LA9_46>='0' && LA9_46<='9')||(LA9_46>='A' && LA9_46<='Z')||LA9_46=='_'||(LA9_46>='a' && LA9_46<='z')) ) {
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
            break;
        case 'I':
            {
            int LA9_13 = input.LA(2);

            if ( (LA9_13=='N') ) {
                int LA9_35 = input.LA(3);

                if ( ((LA9_35>='0' && LA9_35<='9')||(LA9_35>='A' && LA9_35<='Z')||LA9_35=='_'||(LA9_35>='a' && LA9_35<='z')) ) {
                    alt9=21;
                }
                else {
                    alt9=13;}
            }
            else {
                alt9=21;}
            }
            break;
        case 'L':
            {
            int LA9_14 = input.LA(2);

            if ( (LA9_14=='I') ) {
                int LA9_36 = input.LA(3);

                if ( (LA9_36=='M') ) {
                    int LA9_48 = input.LA(4);

                    if ( (LA9_48=='I') ) {
                        int LA9_59 = input.LA(5);

                        if ( (LA9_59=='T') ) {
                            int LA9_69 = input.LA(6);

                            if ( ((LA9_69>='0' && LA9_69<='9')||(LA9_69>='A' && LA9_69<='Z')||LA9_69=='_'||(LA9_69>='a' && LA9_69<='z')) ) {
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
            else {
                alt9=21;}
            }
            break;
        case 'O':
            {
            switch ( input.LA(2) ) {
            case 'R':
                {
                int LA9_37 = input.LA(3);

                if ( (LA9_37=='D') ) {
                    int LA9_49 = input.LA(4);

                    if ( (LA9_49=='E') ) {
                        int LA9_60 = input.LA(5);

                        if ( (LA9_60=='R') ) {
                            int LA9_70 = input.LA(6);

                            if ( ((LA9_70>='0' && LA9_70<='9')||(LA9_70>='A' && LA9_70<='Z')||LA9_70=='_'||(LA9_70>='a' && LA9_70<='z')) ) {
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
                else {
                    alt9=21;}
                }
                break;
            case 'F':
                {
                int LA9_38 = input.LA(3);

                if ( (LA9_38=='F') ) {
                    int LA9_50 = input.LA(4);

                    if ( (LA9_50=='S') ) {
                        int LA9_61 = input.LA(5);

                        if ( (LA9_61=='E') ) {
                            int LA9_71 = input.LA(6);

                            if ( (LA9_71=='T') ) {
                                int LA9_79 = input.LA(7);

                                if ( ((LA9_79>='0' && LA9_79<='9')||(LA9_79>='A' && LA9_79<='Z')||LA9_79=='_'||(LA9_79>='a' && LA9_79<='z')) ) {
                                    alt9=21;
                                }
                                else {
                                    alt9=15;}
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
        case 'P':
            {
            int LA9_16 = input.LA(2);

            if ( (LA9_16=='L') ) {
                int LA9_39 = input.LA(3);

                if ( (LA9_39=='A') ) {
                    int LA9_51 = input.LA(4);

                    if ( (LA9_51=='N') ) {
                        int LA9_62 = input.LA(5);

                        if ( ((LA9_62>='0' && LA9_62<='9')||(LA9_62>='A' && LA9_62<='Z')||LA9_62=='_'||(LA9_62>='a' && LA9_62<='z')) ) {
                            alt9=21;
                        }
                        else {
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
        case 'S':
            {
            int LA9_17 = input.LA(2);

            if ( (LA9_17=='E') ) {
                switch ( input.LA(3) ) {
                case 'T':
                    {
                    int LA9_52 = input.LA(4);

                    if ( ((LA9_52>='0' && LA9_52<='9')||(LA9_52>='A' && LA9_52<='Z')||LA9_52=='_'||(LA9_52>='a' && LA9_52<='z')) ) {
                        alt9=21;
                    }
                    else {
                        alt9=19;}
                    }
                    break;
                case 'L':
                    {
                    int LA9_53 = input.LA(4);

                    if ( (LA9_53=='E') ) {
                        int LA9_64 = input.LA(5);

                        if ( (LA9_64=='C') ) {
                            int LA9_73 = input.LA(6);

                            if ( (LA9_73=='T') ) {
                                int LA9_80 = input.LA(7);

                                if ( ((LA9_80>='0' && LA9_80<='9')||(LA9_80>='A' && LA9_80<='Z')||LA9_80=='_'||(LA9_80>='a' && LA9_80<='z')) ) {
                                    alt9=21;
                                }
                                else {
                                    alt9=18;}
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
            else {
                alt9=21;}
            }
            break;
        case 'W':
            {
            int LA9_18 = input.LA(2);

            if ( (LA9_18=='H') ) {
                int LA9_41 = input.LA(3);

                if ( (LA9_41=='E') ) {
                    int LA9_54 = input.LA(4);

                    if ( (LA9_54=='R') ) {
                        int LA9_65 = input.LA(5);

                        if ( (LA9_65=='E') ) {
                            int LA9_74 = input.LA(6);

                            if ( ((LA9_74>='0' && LA9_74<='9')||(LA9_74>='A' && LA9_74<='Z')||LA9_74=='_'||(LA9_74>='a' && LA9_74<='z')) ) {
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
            break;
        case 'A':
        case 'C':
        case 'H':
        case 'J':
        case 'K':
        case 'M':
        case 'N':
        case 'Q':
        case 'R':
        case 'T':
        case 'U':
        case 'V':
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
        case '\t':
        case '\n':
        case '\r':
        case ' ':
            {
            alt9=24;
            }
            break;
        case '-':
        case '/':
            {
            alt9=25;
            }
            break;
        case ',':
            {
            alt9=27;
            }
            break;
        case '{':
            {
            alt9=28;
            }
            break;
        case '}':
            {
            alt9=29;
            }
            break;
        case ';':
            {
            alt9=30;
            }
            break;
        default:
            NoViableAltException nvae =
                new NoViableAltException("1:1: Tokens : ( T47 | T48 | T49 | T50 | T51 | T52 | T53 | K_BY | K_DELETE | K_EXPLAIN | K_FROM | K_GET | K_IN | K_LIMIT | K_OFFSET | K_ORDER | K_PLAN | K_SELECT | K_SET | K_WHERE | Identifier | StringLiteral | IntegerLiteral | WS | COMMENT | ASSOC | COMMA | LEFT_BRACE | RIGHT_BRACE | SEMICOLON );", 9, 0, input);

            throw nvae;
        }

        switch (alt9) {
            case 1 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:10: T47
                {
                mT47(); 

                }
                break;
            case 2 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:14: T48
                {
                mT48(); 

                }
                break;
            case 3 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:18: T49
                {
                mT49(); 

                }
                break;
            case 4 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:22: T50
                {
                mT50(); 

                }
                break;
            case 5 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:26: T51
                {
                mT51(); 

                }
                break;
            case 6 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:30: T52
                {
                mT52(); 

                }
                break;
            case 7 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:34: T53
                {
                mT53(); 

                }
                break;
            case 8 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:38: K_BY
                {
                mK_BY(); 

                }
                break;
            case 9 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:43: K_DELETE
                {
                mK_DELETE(); 

                }
                break;
            case 10 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:52: K_EXPLAIN
                {
                mK_EXPLAIN(); 

                }
                break;
            case 11 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:62: K_FROM
                {
                mK_FROM(); 

                }
                break;
            case 12 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:69: K_GET
                {
                mK_GET(); 

                }
                break;
            case 13 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:75: K_IN
                {
                mK_IN(); 

                }
                break;
            case 14 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:80: K_LIMIT
                {
                mK_LIMIT(); 

                }
                break;
            case 15 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:88: K_OFFSET
                {
                mK_OFFSET(); 

                }
                break;
            case 16 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:97: K_ORDER
                {
                mK_ORDER(); 

                }
                break;
            case 17 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:105: K_PLAN
                {
                mK_PLAN(); 

                }
                break;
            case 18 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:112: K_SELECT
                {
                mK_SELECT(); 

                }
                break;
            case 19 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:121: K_SET
                {
                mK_SET(); 

                }
                break;
            case 20 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:127: K_WHERE
                {
                mK_WHERE(); 

                }
                break;
            case 21 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:135: Identifier
                {
                mIdentifier(); 

                }
                break;
            case 22 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:146: StringLiteral
                {
                mStringLiteral(); 

                }
                break;
            case 23 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:160: IntegerLiteral
                {
                mIntegerLiteral(); 

                }
                break;
            case 24 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:175: WS
                {
                mWS(); 

                }
                break;
            case 25 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:178: COMMENT
                {
                mCOMMENT(); 

                }
                break;
            case 26 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:186: ASSOC
                {
                mASSOC(); 

                }
                break;
            case 27 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:192: COMMA
                {
                mCOMMA(); 

                }
                break;
            case 28 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:198: LEFT_BRACE
                {
                mLEFT_BRACE(); 

                }
                break;
            case 29 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:209: RIGHT_BRACE
                {
                mRIGHT_BRACE(); 

                }
                break;
            case 30 :
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:1:221: SEMICOLON
                {
                mSEMICOLON(); 

                }
                break;

        }

    }


 

}