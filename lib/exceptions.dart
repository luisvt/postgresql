part of postgresql;

class _PgClientException extends SqlClientException {
  _PgClientException(String msg, [error]) : super(msg, error);

  String toString() => error == null ? msg : '$msg ($error)';
}

class _PgServerException extends SqlServerException with SqlException, Exception {
  
  _PgServerException(SqlServerInformation info) : super(info);

  String toString() {
    var p = position == null ? '' : ' (position: $position)';
    return 'PostgreSQL $severity $code $message$p';
  }
}

class _PgServerInformation extends SqlServerInformation {

  _PgServerInformation(bool isError, Map<String, String> map) :
    super(isError, 
        map['C'] == null ? '' : map['C'],
        map['S'] == null ? '' : map['S'],
        map['M'] == null ? '' : map['M'],
        map['D'] == null ? '' : map['D'],
        map['P'] == null ? null : int.parse(map['P'], onError: (_) => null),
        map.keys.fold('', (val, item) {
          var fieldName = _fieldNames[item] == null ? item : _fieldNames[item];
          var fieldValue = map[item];
          return '$val\n$fieldName: $fieldValue';
        }));
}

final Map<String, String> _fieldNames = {
  'S': 'Severity',
  'C': 'Code',
  'M': 'Message',
  'D': 'Detail',
  'H': 'Hint',
  'P': 'Position',
  'p': 'Internal position',
  'q': 'Internal query',
  'W': 'Where',
  'F': 'File',
  'L': 'Line',
  'R': 'Routine'
};
