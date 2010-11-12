grammar Cql;

options {
    language = Java;
}

@header {
    package org.apache.cassandra.cql;
    import java.util.ArrayList;
    import org.apache.cassandra.thrift.ConsistencyLevel;
    import org.apache.cassandra.avro.InvalidRequestException;
}

@members {
    private List<String> recognitionErrors = new ArrayList<String>();
    
    public void displayRecognitionError(String[] tokenNames, RecognitionException e)
    {
        String hdr = getErrorHeader(e);
        String msg = getErrorMessage(e, tokenNames);
        recognitionErrors.add(hdr + " " + msg);
    }
    
    public List<String> getRecognitionErrors()
    {
        return recognitionErrors;
    }
    
    public void throwLastRecognitionError() throws InvalidRequestException
    {
        if (recognitionErrors.size() > 0)
        {
            InvalidRequestException invalidExcep = new InvalidRequestException();
            invalidExcep.why = recognitionErrors.get((recognitionErrors.size()-1));
            throw invalidExcep;
        }
    }
}

@lexer::header {
    package org.apache.cassandra.cql;
}

query returns [CQLStatement stmnt]
    : selectStatement { $stmnt = new CQLStatement(StatementType.SELECT, $selectStatement.expr); }
    | updateStatement { $stmnt = new CQLStatement(StatementType.UPDATE, $updateStatement.expr); }
    | useStatement    { $stmnt = new CQLStatement(StatementType.USE, $useStatement.keyspace); }
    ;

// USE <KEYSPACE>;
useStatement returns [String keyspace]
    : K_USE IDENT { $keyspace = $IDENT.text; } endStmnt
    ;

/**
 * SELECT FROM
 *     <CF>
 * USING
 *     CONSISTENCY.ONE
 * WHERE
 *     KEY = "key1" AND KEY = "key2" AND
 *     COL > 1 AND COL < 100
 * COLLIMIT 10 DESC;
 */
selectStatement returns [SelectStatement expr]
    : { 
          int numRecords = 10000;
          SelectExpression expression = null;
          boolean isCountOp = false;
          ConsistencyLevel cLevel = ConsistencyLevel.ONE;
      }
      K_SELECT
          ( s1=selectExpression                 { expression = s1; }
          | K_COUNT '(' s2=selectExpression ')' { expression = s2; isCountOp = true; }
          )
          K_FROM columnFamily=IDENT
          ( K_USING K_CONSISTENCY '.' K_LEVEL { cLevel = ConsistencyLevel.valueOf($K_LEVEL.text); } )?
          ( K_WHERE whereClause )?
          ( K_LIMIT rows=INTEGER { numRecords = Integer.parseInt($rows.text); } )?
          endStmnt
      {
          return new SelectStatement(expression,
                                     isCountOp,
                                     $columnFamily.text,
                                     cLevel,
                                     $whereClause.clause,
                                     numRecords);
      }
    ;

/**
 * UPDATE
 *     <CF>
 * USING
 *     CONSISTENCY.ONE
 * WITH
 *     ROW("key1", COL("col1", "val1"), ...) AND
 *     ROW("key2", COL("col1", "val1"), ...) AND
 *     ROW("key3", COLUMN("col1", "val1"), ...)
 */
updateStatement returns [UpdateStatement expr]
    : { ConsistencyLevel cLevel = ConsistencyLevel.ONE; }
      K_UPDATE IDENT
          (K_USING K_CONSISTENCY '.' K_LEVEL { cLevel = ConsistencyLevel.valueOf($K_LEVEL.text); })?
          K_WITH first=rowDef { $expr = new UpdateStatement($IDENT.text, first, cLevel); }
          (K_AND next=rowDef { $expr.and(next); })* endStmnt
    ;

// TODO: date/time, utf8
term returns [Term item]
    : ( t=STRING_LITERAL | t=LONG )
      { $item = new Term($t.text, $t.type); }
    ;

// Note: ranges are inclusive so >= and >, and < and <= all have the same semantics.  
relation returns [Relation rel]
    : { Term entity = new Term("KEY", STRING_LITERAL); }
      (K_KEY | name=term { entity = $name.item; } ) type=('=' | '<' | '<=' | '>=' | '>') t=term
      { return new Relation(entity, $type.text, $t.item); }
    ;

// relation [[AND relation] ...]
whereClause returns [WhereClause clause]
    : first=relation { $clause = new WhereClause(first); } 
          (K_AND next=relation { $clause.and(next); })*
    ;

// [FIRST n] [REVERSED] name1[[[,name2],nameN],...]
// [FIRST n] [REVERSED] name1..nameN
selectExpression returns [SelectExpression expr]
    : {
          int count = 10000;
          boolean reversed = false;
      }
      ( K_FIRST cols=INTEGER { count = Integer.parseInt($cols.text); } )?
      ( K_REVERSED { reversed = true; } )?
      ( first=term { $expr = new SelectExpression(first, count, reversed); }
            (',' next=term { $expr.and(next); })*
      | start=term '..' finish=term { $expr = new SelectExpression(start, finish, count, reversed); }
      )
    ;

columnDef returns [Column column]
    : K_COLUMN '(' n=term ',' v=term ')' { $column = new Column($n.item, $v.item); }
    ;

rowDef returns [Row row]
    : K_ROW '(' key=term ',' first=columnDef { $row = new Row($key.item, first); }
          (',' next=columnDef { $row.and(next); })* ')'
    ;
    
endStmnt
    : (EOF | ';')
    ;

// Case-insensitive keywords
K_SELECT:      S E L E C T;
K_FROM:        F R O M;
K_WHERE:       W H E R E;
K_AND:         A N D;
K_KEY:         K E Y;
K_COLUMN:      C O L (U M N)?;
K_UPDATE:      U P D A T E;
K_WITH:        W I T H;
K_ROW:         R O W;
K_LIMIT:       L I M I T;
K_USING:       U S I N G;
K_CONSISTENCY: C O N S I S T E N C Y;
K_LEVEL:       ( O N E 
               | Q U O R U M 
               | A L L 
               | D C Q U O R U M 
               | D C Q U O R U M S Y N C
               )
               ;
K_USE:         U S E;
K_FIRST:       F I R S T;
K_REVERSED:    R E V E R S E D;
K_COUNT:       C O U N T;

// Case-insensitive alpha characters
fragment A: ('a'|'A');
fragment B: ('b'|'B');
fragment C: ('c'|'C');
fragment D: ('d'|'D');
fragment E: ('e'|'E');
fragment F: ('f'|'F');
fragment G: ('g'|'G');
fragment H: ('h'|'H');
fragment I: ('i'|'I');
fragment J: ('j'|'J');
fragment K: ('k'|'K');
fragment L: ('l'|'L');
fragment M: ('m'|'M');
fragment N: ('n'|'N');
fragment O: ('o'|'O');
fragment P: ('p'|'P');
fragment Q: ('q'|'Q');
fragment R: ('r'|'R');
fragment S: ('s'|'S');
fragment T: ('t'|'T');
fragment U: ('u'|'U');
fragment V: ('v'|'V');
fragment W: ('w'|'W');
fragment X: ('x'|'X');
fragment Y: ('y'|'Y');
fragment Z: ('z'|'Z');
    
STRING_LITERAL
    : '"'
      { StringBuilder b = new StringBuilder(); }
      ( c=~('"'|'\r'|'\n') { b.appendCodePoint(c);}
      | '"' '"'            { b.appendCodePoint('"');}
      )*
      '"'
      { setText(b.toString()); }
    ;

fragment DIGIT
    : '0'..'9'
    ;

fragment LETTER
    : ('A'..'Z' | 'a'..'z')
    ;

INTEGER
    : DIGIT+
    ;
    
LONG
    : INTEGER 'L' { setText($INTEGER.text); }
    ;

IDENT
    : LETTER (LETTER | DIGIT)*
    ;
    
WS
    : (' ' | '\t' | '\n' | '\r')+ { $channel = HIDDEN; }
    ;

COMMENT
    : ('--' | '//') .* ('\n'|'\r') { $channel = HIDDEN; }
    ;
    
MULTILINE_COMMENT
    : '/*' .* '*/' { $channel = HIDDEN; }
    ;
