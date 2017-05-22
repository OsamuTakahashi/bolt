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

TRUE    : T R U E
        ;

FALSE   : F A L S E
        ;

NULL    : N U L L
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

ID      : [a-zA-Z_][a-zA-Z0-9_]*
        ;

NUMBER  : ('+'|'-')? [0-9]+  (F | D)?
        | ('+'|'-')? [0-9]* '.' [0-9]* (E ('+' | '-')? [0-9]+)? (F | D)?
        ;

STRING  : '\'' ~[\']* '\''
        ;

WS      : [ \t\r\n]+ -> skip
        ;
