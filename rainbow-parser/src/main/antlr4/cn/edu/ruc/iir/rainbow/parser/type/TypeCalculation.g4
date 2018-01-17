//TODO: consider using the SQL grammar for this
grammar TypeCalculation;

// workaround for:
//  https://github.com/antlr/antlr4/issues/118
typeCalculation
    : expression EOF
    ;

expression
    : NULL                                                              #nullLiteral
    | INTEGER_VALUE                                                     #numericLiteral
    | binaryFunctionName '(' left=expression ',' right=expression ')'   #binaryFunction
    | IDENTIFIER                                                        #identifier
    | '(' expression ')'                                                #parenthesizedExpression
    | operator=(MINUS | PLUS) expression                                #arithmeticUnary
    | left=expression operator=(ASTERISK | SLASH) right=expression      #arithmeticBinary
    | left=expression operator=(PLUS | MINUS) right=expression          #arithmeticBinary
    ;

binaryFunctionName
    : name=(MAX | MIN)
    ;

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
NULL: 'NULL';
MIN: 'MIN';
MAX: 'MAX';

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_' )*
    ;

INTEGER_VALUE
    : DIGIT+
    ;

fragment DIGIT
    : ('0'..'9')
    ;

fragment LETTER
    : [A-Za-z]
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;
