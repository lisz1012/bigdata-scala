grammar aaa;


aaainit  : '{' value (','  value)*  '}';

//value : value ('+'|'-') value | INT;

value    :   INT
         |   aaainit;

INT      :   [0-9]+;
WS       :   [ \t\r\n]+ -> skip;