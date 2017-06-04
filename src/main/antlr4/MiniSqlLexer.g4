lexer grammar MiniSqlLexer;

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

CREATE  : C R E A T E
        ;

ALTER   : A L T E R
        ;

DROP    : D R O P
        ;

SELECT  : S E L E C T
        ;

INSERT  : I N S E R T
        ;

UPDATE  : U P D A T E
        ;

DELETE  : D E L E T E
        ;

SHOW    : S H O W
        ;

VALUES  : V A L U E S
        ;

WHERE   : W H E R E
        ;

INTO    : I N T O
        ;

SET     : S E T
        ;

LIMIT   : L I M I T
        ;

FROM    : F R O M
        ;

INDEX   : I N D E X
        ;

FULL    : F U L L
        ;

COLUMNS : C O L U M N S
        ;

OR      : O R
        | '||'
        ;

AND     : A N D
        | '&&'
        ;

NOT     : N O T
        | '!'
        ;

XOR     : X O R
        ;

LIKE    : L I K E
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

TABLES  : T A B L E S
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

CASCADE : C S C A D E
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

MAX     : M A X
        ;

EQ      : '='
        ;

NEQ     : '<>'
        ;

LEQ     : '<='
        ;

GEQ     : '=>'
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

AR      : '*'
        ;

MIN     : '-'
        ;

SP      : '#'
        ;

ID      : [a-zA-Z_][a-zA-Z0-9_]*
        ;

NUMBER  : ('+'|'-')? [0-9]+  (F | D)?
        | ('+'|'-')? [0-9]* '.' [0-9]* (E ('+' | '-')? [0-9]+)? (F | D)?
        ;

STRING  : '\'' ~[\']* '\''
        ;

WS      : [ \t]+ -> channel(HIDDEN)
        ;

NL      : [\r\n] { setText(" "); } -> channel(HIDDEN)
        ;

COMM    : '#' ~[\r\n]* { setText(" "); } -> channel(HIDDEN)
        ;

COMM2   : '--' ~[\r\n]* { setText(" "); } -> channel(HIDDEN)
        ;
