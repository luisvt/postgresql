part of postgresql;

const int _apos = 39;
const int _return = 13;
const int _newline = 10;
const int _backslash = 92;

class PgSqlFormatter extends SqlFormatter {
  String format(value, [String type]) {
    if(value is String) 
      return 'E'+super.format(value);
    
    return super.format(value, type);
  }
}