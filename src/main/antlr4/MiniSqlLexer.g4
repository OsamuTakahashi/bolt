lexer grammar MiniSqlLexer;

@header {
import org.apache.commons.text.StringEscapeUtils;
}

fragment A:('a'|'A');
fragment B:('b'|'B');
fragment C:('c'|'C');
fragment D:('d'|'D');
fragment E:('e'|'E');
fragment F:('f'|'F');
fragment G:('g'|'G');
fragment H:('h'|'H');
fragment I:('i'|'I');
fragment J:('j'|'J');
fragment K:('k'|'K');
fragment L:('l'|'L');
fragment M:('m'|'M');
fragment N:('n'|'N');
fragment O:('o'|'O');
fragment P:('p'|'P');
fragment Q:('q'|'Q');
fragment R:('r'|'R');
fragment S:('s'|'S');
fragment T:('t'|'T');
fragment U:('u'|'U');
fragment V:('v'|'V');
fragment W:('w'|'W');
fragment X:('x'|'X');
fragment Y:('y'|'Y');
fragment Z:('z'|'Z');

fragment CommonCharacter
    : SimpleEscapeSequence
    | OctEscapeSequence
    | HexEscapeSequence
    | UnicodeEscapeSequence
    ;

fragment SimpleEscapeSequence
    : '\\\''
    | '\\"'
    | '\\`'
    | '\\\\'
    | '\\a'
    | '\\b'
    | '\\f'
    | '\\n'
    | '\\r'
    | '\\t'
    | '\\v'
    ;

fragment OctEscapeSequence
    : '\\' [0-7][0-7][0-7]
    ;

fragment HexEscapeSequence
    : '\\x' [0-9a-fA-F]
    | '\\x' [0-9a-fA-F][0-9a-fA-F]
    | '\\x' [0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]
    | '\\x' [0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]
    ;

fragment UnicodeEscapeSequence
    : '\\u' [0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]
    | '\\U' [0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]
            [0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]
    ;
    
ADD     : A D D
        ;

ALL     : A L L
        ;

ALTER   : A L T E R
        ;

AS      : A S
        ;

BY      : B Y
        ;

CAST    : C A S T
        ;

COLUMN  : C O L U M N
        ;

CREATE  : C R E A T E
        ;

CROSS   : C R O S S
        ;

DELETE  : D E L E T E
        ;

DESCRIBE: D E S C R I B E
        ;

DROP    : D R O P
        ;

EXISTS  : E X I S T S
        ;

EXTRACT : E X T R A C T
        ;

GROUP   : G R O U P
        ;

HAVING  : H A V I N G
        ;

INDEX   : I N D E X
        ;

IF      : I F
        ;

INSERT  : I N S E R T
        ;

INTERVAL: I N T E R V A L
        ;

INTO    : I N T O
        ;

JOIN    : J O I N
        ;

LIMIT   : L I M I T
        ;

NOT     : N O T
        | '!'
        ;

OFFSET  : O F F S E T
        ;

ORDER   : O R D E R
        ;

ORDINAL : O R D I N A L
        ;

SELECT  : S E L E C T
        ;

SET     : S E T
        ;

SHOW    : S H O W
        ;

UNION   : U N I O N
        ;

UPDATE  : U P D A T E
        ;

USE     : U S E
        ;

USING   : U S I N G
        ;

VALUES  : V A L U E S
        ;

WHERE   : W H E R E
        ;

FROM    : F R O M
        ;

DISTINCT: D I S T I N C T
        ;

FULL    : F U L L
        ;

INNER   : I N N E R
        ;

OUTER   : O U T E R
        ;

LEFT    : L E F T
        ;

RIGHT   : R I G H T
        ;

HASH    : H A S H
        ;

UNNEST  : U N N E S T
        ;

OR      : O R
        | '||'
        ;

AND     : A N D
        | '&&'
        ;

XOR     : X O R
        ;

IS      : I S
        ;

LIKE    : L I K E
        ;

BETWEEN : B E T W E E N
        ;

WITH    : W I T H
        ;

IN      : I N
        ;

ON      : O N
        ;

TRUE    : T R U E
        ;

FALSE   : F A L S E
        ;

NULL    : N U L L
        ;

DATABASE: D A T A B A S E
        ;

TABLE   : T A B L E
        ;

PRIMARY : P R I M A R Y
        ;

KEY     : K E Y
        ;

INTERLEAVE
        : I N T E R L E A V E
        ;

PARENT  : P A R E N T
        ;

CASCADE : C A S C A D E
        ;

NO      : N O
        ;

ACTION  : A C T I O M
        ;

UNIQUE  : U N I Q U E
        ;

NULL_FILTERED
        : N U L L '_' F I L T E R E D
        ;

STORING : S T O R I N G
        ;

ASC     : A S C
        ;

DESC    : D E S C
        ;

FORCE_INDEX
        : F O R C E '_' I N D E X
        ;

FORCE_JOIN_ORDER
        : F O R C E '_' J O I N '_' O R D E R
        ;

JOIN_TYPE
        : J O I N '_' T Y P E
        ;

BOOL    : B O O L
        ;

INT64   : I N T '64'
        ;

FLOAT64 : F L O A T '64'
        ;

STRING_TYPE
        : S T R I N G
        ;

BYTES   : B Y T E S
        ;

DATE    : D A T E
        ;

TIMESTAMP
        : T I M E S T A M P
        ;

ARRAY   : A R R A Y
        ;

STRUCT  : S T R U C T
        ;

//MAX     : M A X
//        ;

PLUS    : '+'
        ;

MINUS   : '-'
        ;

MUL     : '*'
        ;

DIV     : '/'
        ;

EQ      : '='
        ;

BIT_SL  : '<<'
        ;

BIT_SR  : '>>'
        ;

BIT_AND : '&'
        ;

BIT_OR  : '|'
        ;

BIT_XOR : '^'
        ;

BIT_NOT : '~'
        ;

NEQ     : '<>'
        | '!='
        ;

LEQ     : '<='
        ;

GEQ     : '>='
        ;

LT      : '<'
        ;

GT      : '>'
        ;

BG      : '('
        ;

ED      : ')'
        ;

SC      : ';'
        ;

CN      : ','
        ;

DOT     : '.'
        ;

AR      : '*'
        ;

MIN     : '-'
        ;

SP      : '#'
        ;

AT      : '@'
        ;

BRB     : '{'
        ;

BRE     : '}'
        ;

BRACE_B : '['
        ;

BRACE_E : ']'
        ;

ID      : [a-zA-Z_][a-zA-Z0-9_]*
        ;

INT_VAL : '0x' [0-9a-fA-F]+
        | ([1-9][0-9]*)? [0-9]
        ;

DBL_VAL : (([1-9][0-9]*)? [0-9])? '.' [0-9]+ (E ('+'|'-')? [0-9]+)?
        | ([1-9][0-9]*)? [0-9] E ('+'|'-')? [0-9]+
        ;


STRING  : '\'' (~['\\\r\n\u0085\u2028\u2029] | CommonCharacter)* '\'' { setText("'" + StringEscapeUtils.unescapeJava(getText().substring(1, getText().length()-1)) + "'"); }
        | '"' (~["\\\r\n\u0085\u2028\u2029] | CommonCharacter)* '"'  { setText("\"" + StringEscapeUtils.unescapeJava(getText().substring(1, getText().length()-1)) + "\""); }
        ;

WS      : [ \t]+ -> channel(HIDDEN)
        ;

NL      : [\r\n] { setText(" "); } -> channel(HIDDEN)
        ;

COMM    : '#' ~[\r\n]* { setText(" "); } -> channel(HIDDEN)
        ;

COMM2   : '--' ~[\r\n]* { setText(" "); } -> channel(HIDDEN)
        ;
