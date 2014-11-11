part of postgresql;

class PgConnection extends Connection {

//  PgConnection(this._socket, Settings settings)
//    : _userName = settings.user,
//      _passwordHash = _md5s(settings.password + settings.user),
//      _databaseName = settings.database;

  _State state = _State.NOT_CONNECTED;
  
  Transaction _transactionStatus = Transaction.TRANSACTION_UNKNOWN;

  PgConnection(String userName, String password, String dbName,
   {
     String host: '127.0.0.1',
     int port: 5432,
     int maxPacketSize: 16 * 1024 * 1024,
//      bool useCompression: false,
     bool useSSL: false,
     int timeout
   }) : super(userName, _md5s(password + userName), dbName, host: host, port: port, maxPacketSize: maxPacketSize, useSSL: useSSL, timeout: timeout);
  
  Transaction get transactionStatus => _transactionStatus;

  Socket _socket;
  final _Buffer _buffer = new _Buffer();
  bool _hasConnected = false;
  Completer _connected = new Completer();
  final Completer _closed = new Completer();
  final Queue<_Query> _sendQueryQueue = new Queue<_Query>();
  _Query _query;
  int _msgType;
  int _msgLength;

  Stream get unhandled => _unhandled.stream;
  final StreamController _unhandled = new StreamController();

  Future<PgConnection> connect() {
    _connected = new Completer();
    return new Future.sync(() {

      Future<Socket> future = useSSL
        ? _connectSsl()
        : Socket.connect(host, port);

      return future.then((socket) {
        _socket = socket;
        _socket.listen(_readData, onError: _handleSocketError, onDone: _handleSocketClosed);
        state = _State.SOCKET_CONNECTED;
        _sendStartupMessage();
        return _connected.future;
      });
    });
  }

  static String _md5s(String s) {
    var hash = new MD5();
    hash.add(s.codeUnits.toList());
    return CryptoUtils.bytesToHex(hash.close());
  }

  Future<SecureSocket> _connectSsl() {

    var completer = new Completer<SecureSocket>();

    Socket.connect(host, port).then((socket) {

      socket.listen((data) {
        if (data == null || data[0] != _S) {
          socket.close();
          completer.completeError('This postgresql server is not configured to support SSL connections.');
        } else {
          // TODO validate certs
          new Future.sync(() => SecureSocket.secure(socket, onBadCertificate: (cert) => true))
            .then((s) => completer.complete(s))
            .catchError((e) => completer.completeError(e));
        }
      });

      // Write header, and SSL magic number.
      socket.add([0, 0, 0, 8, 4, 210, 22, 47]);

    })
    .catchError((e) {
      completer.completeError(e);
    });

    return completer.future;
  }

  void _sendStartupMessage() {
    if (state != _State.SOCKET_CONNECTED)
      throw new StateError('Invalid state during startup.');

    var msg = new _MessageBuffer();
    msg.addInt32(0); // Length padding.
    msg.addInt32(_PROTOCOL_VERSION);
    msg.addString('user');
    msg.addString(userName);
    msg.addString('database');
    msg.addString(dbName);
    //TODO write params list.
    msg.addByte(0);
    msg.setLength(startup: true);

    _socket.add(msg.buffer);

    state = _State.AUTHENTICATING;
  }

  void _readAuthenticationRequest(int msgType, int length) {
    assert(_buffer.bytesAvailable >= length);

    if (state != _State.AUTHENTICATING)
      throw new StateError('Invalid connection state while authenticating.');

    int authType = _buffer.readInt32();

    if (authType == _AUTH_TYPE_OK) {
      state = _State.AUTHENTICATED;
      return;
    }

    // Only MD5 authentication is supported.
    if (authType != _AUTH_TYPE_MD5) {
      throw new _PgClientException('Unsupported or unknown authentication type: ${_authTypeAsString(authType)}, only MD5 authentication is supported.');
    }

    var bytes = _buffer.readBytes(4);
    var salt = new String.fromCharCodes(bytes);
    var md5 = 'md5' + _md5s('${password}$salt');

    // Build message.
    var msg = new _MessageBuffer();
    msg.addByte(_MSG_PASSWORD);
    msg.addInt32(0);
    msg.addString(md5);
    msg.setLength();

    _socket.add(msg.buffer);
  }

  void _readReadyForQuery(int msgType, int length) {
    assert(_buffer.bytesAvailable >= length);

    int c = _buffer.readByte();

    // print('Ready for query. Transaction state: ${_itoa(c)}');

    // TODO store transaction state somewhere. Perhaps this needs to be able
    // to be read via the api. Perhaps Connection.transactionState, or just Connection.state?
    // which is one of: {BUSY, IDLE, BEGUN, ERROR};

    if (c == _I || c == _T || c == _E) {

      if (c == _I)
        _transactionStatus = Transaction.TRANSACTION_NONE;
      else if (c == _T)
        _transactionStatus = Transaction.TRANSACTION_BEGUN;
      else if (c == _E)
        _transactionStatus = Transaction.TRANSACTION_ERROR;

      var was = state;

      state = _State.IDLE;

      if (_query != null) {
        _query.close();
        _query = null;
      }

      if (was == _State.AUTHENTICATED) {
        _hasConnected = true;
        _connected.complete(this);
      }

      //FIXME Dear deep thought, what is the idiomatic way to do this?
      new Future.value(42).then((_) => _processSendQueryQueue());

    } else {
      _destroy();
      throw new _PgClientException('Unknown ReadyForQuery transaction status: ${_itoa(c)}.');
    }
  }

  void _handleSocketError(error) {

    if (state == _State.CLOSED) {
      //FIXME logging
      print('Error after socket closed: $error');
      _destroy();
      return;
    }

    _destroy();

    var ex = new _PgClientException('Socket error.', error);

    if (!_hasConnected) {
      _connected.completeError(ex);
    } else if (_query != null) {
      _query.addError(ex);
    } else {
      _unhandled.add(ex);
    }
  }

  void _handleSocketClosed() {
    if (state != _State.CLOSED) {
      _handleSocketError(new _PgClientException('Socket closed.'));
    }
  }

  void _readData(List<int> data) {

    try {

      if (state == _State.CLOSED)
        return;

      _buffer.append(data);

      // Handle resuming after storing message type and length.
      if (_msgType != null) {
        if (_msgLength > _buffer.bytesAvailable)
            return; // Wait for entire message to be in buffer.

        _readMessage(_msgType, _msgLength);

        _msgType = null;
        _msgLength = null;
      }

      // Main message loop.
      while (state != _State.CLOSED) {

        if (_buffer.bytesAvailable < 5)
          return; // Wait for more data.

        // Message length is the message length excluding the message type code, but
        // including the 4 bytes for the length fields. Only the length of the body
        // is passed to each of the message handlers.
        int msgType = _buffer.readByte();
        int length = _buffer.readInt32() - 4;

        if (!_checkMessageLength(msgType, length + 4)) {
          throw new _PgClientException('Lost message sync.');
        }

        if (length > _buffer.bytesAvailable) {
          // Wait for entire message to be in buffer.
          // Store type, and length for when more data becomes available.
          _msgType =  msgType;
          _msgLength = length;
          return;
        }

        _readMessage(msgType, length);
      }

    } on Exception catch (e) {
      _destroy();
      throw new _PgClientException('Error reading data.', e); //TODO test that this will be caught by unhandled stream.
    }
  }

  bool _checkMessageLength(int msgType, int msgLength) {

    if (state == _State.AUTHENTICATING) {
      if (msgLength < 8) return false;
      if (msgType == _MSG_AUTH_REQUEST && msgLength > 2000) return false;
      if (msgType == _MSG_ERROR_RESPONSE && msgLength > 30000) return false;
    } else {
      if (msgLength < 4) return false;

      // These are the only messages from the server which may exceed 30,000
      // bytes.
      if (msgLength > 30000 && (msgType != _MSG_NOTICE_RESPONSE
          && msgType != _MSG_ERROR_RESPONSE
          && msgType != _MSG_COPY_DATA
          && msgType != _MSG_ROW_DESCRIPTION
          && msgType != _MSG_DATA_ROW
          && msgType != _MSG_FUNCTION_CALL_RESPONSE
          && msgType != _MSG_NOTIFICATION_RESPONSE)) {
        return false;
      }
    }
    return true;
  }

  void _readMessage(int msgType, int length) {

    int pos = _buffer.bytesRead;

    // print('Handle message: ${_itoa(msgType)} ${_messageName(msgType)}.');

    switch (msgType) {

      case _MSG_AUTH_REQUEST:     _readAuthenticationRequest(msgType, length); break;
      case _MSG_READY_FOR_QUERY:  _readReadyForQuery(msgType, length); break;

      case _MSG_ERROR_RESPONSE:
      case _MSG_NOTICE_RESPONSE:
                                  _readErrorOrNoticeResponse(msgType, length); break;

      case _MSG_BACKEND_KEY_DATA: _readBackendKeyData(msgType, length); break;
      case _MSG_PARAMETER_STATUS: _readParameterStatus(msgType, length); break;

      case _MSG_ROW_DESCRIPTION:  _readRowDescription(msgType, length); break;
      case _MSG_DATA_ROW:         _readDataRow(msgType, length); break;
      case _MSG_EMPTY_QUERY_REPONSE: assert(length == 0); break;
      case _MSG_COMMAND_COMPLETE: _readCommandComplete(msgType, length); break;

      default:
        throw new _PgClientException("Unknown, or unimplemented message: ${UTF8.decode([msgType])}.");
    }

    if (pos + length != _buffer.bytesRead)
      throw new _PgClientException('Lost message sync.');
  }

  void _readErrorOrNoticeResponse(int msgType, int length) {
    assert(_buffer.bytesAvailable >= length);

    var map = new Map<String, String>();
    int errorCode = _buffer.readByte();
    while (errorCode != 0) {
      var msg = _buffer.readString(length); //TODO check length remaining.
      map[new String.fromCharCode(errorCode)] = msg;
      errorCode = _buffer.readByte();
    }

    var info = new _PgServerInformation(
                         msgType == _MSG_ERROR_RESPONSE,
                         map);

    if (msgType == _MSG_ERROR_RESPONSE) {
      var ex = new _PgServerException(info);
      if (!_hasConnected) {
          state = _State.CLOSED;
          _socket.destroy();
          _connected.completeError(ex);
      } else if (_query != null) {
        _query.addError(ex);
      } else {
        _unhandled.add(ex);
      }
    } else {
      _unhandled.add(info);
    }
  }

  void _readBackendKeyData(int msgType, int length) {
    assert(_buffer.bytesAvailable >= length);
    _buffer.readBytes(length);
  }

  void _readParameterStatus(int msgType, int length) {
    assert(_buffer.bytesAvailable >= length);
    _buffer.readBytes(length);
  }

  Stream _errorStream(err) {
    return new Stream.fromFuture(
        new Future.error(err));
  }

  Future<Result> query(String sql, [values]) {
    try {
      if (values != null)
        sql = new PgSqlFormatter().substitute(sql, values);
      var query = _enqueueQuery(sql);
      return query.stream.toList().then((results) {
        print(results);
      });
    } on Exception catch (ex) {
      return new Future.error(ex);
    }
  }

  Future<Result> execute(String sql, [parameters, bool transactional = false, bool consistent = true]) {
    if(state == _State.NOT_CONNECTED) {
      return connect().then((conn) {
        return _execute(sql, parameters, transactional, consistent);
      });
    }
    return _execute(sql, parameters, transactional, consistent);
  }
  
  Future<Result> _execute(String sql, [parameters, bool transactional = false, bool consistent = true]) {
        try {
            if (parameters != null)
              sql = new PgSqlFormatter().substitute(sql, parameters);
            var query = _enqueueQuery(sql);
            return query.stream.toList().then((rows) {
              return new Result()
                ..rows = rows
                ..affectedRows = query._rowsAffected;
//          ..insertedId = query.
            });
                
          } on Exception catch (ex) {
            return new Future.error(ex);
          }
    }

  Future runInTransaction(Future operation(), [Isolation isolation = Isolation.READ_COMMITTED]) {

    var begin = 'begin;';
    switch(isolation) {
      case Isolation.REPEATABLE_READ:
        begin += ' set transaction isolation level repeatable read;';
        break;
      case Isolation.SERIALIZABLE:
        begin += ' set transaction isolation level serializable;';
        
    }

    return execute(begin)
      .then((_) => operation())
      .then((_) => execute('commit'))
      .catchError((e) {
        return execute('rollback')
          .then((_) => new Future.error(e));
      });
  }

  _Query _enqueueQuery(String sql) {

    if (sql == null || sql == '')
      throw new _PgClientException('SQL query is null or empty.');

    if (state == _State.CLOSED)
      throw new _PgClientException('Connection is closed, cannot execute query.');

    var query = new _Query(sql);
    _sendQueryQueue.addLast(query);

    //FIXME What is the idiomatic way to do this?
    new Future.value(42).then((_) => _processSendQueryQueue());

    return query;
  }

  void _processSendQueryQueue() {

    if (_sendQueryQueue.isEmpty)
      return;

    if (_query != null)
      return;

    if (state == _State.CLOSED)
      return;

//    print(state);
    assert(state == _State.IDLE);

    _query = _sendQueryQueue.removeFirst();

    var msg = new _MessageBuffer();
    msg.addByte(_MSG_QUERY);
    msg.addInt32(0); // Length padding.
    msg.addString(_query.sql);
    msg.setLength();

    _socket.add(msg.buffer);

    state = _State.BUSY;
    _query.state = _QueryState.BUSY;
    _transactionStatus = Transaction.TRANSACTION_UNKNOWN;
  }

  void _readRowDescription(int msgType, int length) {

    assert(_buffer.bytesAvailable >= length);

    state = _State.STREAMING;

    int count = _buffer.readInt16();
    var list = new List<_Column>(count);

    for (int i = 0; i < count; i++) {
      var name = _buffer.readString(length); //FIXME better maxSize.
      switch(name) {
        case '?column?':
        case 'bool':
          name = 'column$i';
      }
      int fieldId = _buffer.readInt32();
      int tableColNo = _buffer.readInt16();
      int fieldType = _buffer.readInt32();
      int dataSize = _buffer.readInt16();
      int typeModifier = _buffer.readInt32();
      int formatCode = _buffer.readInt16();

      list[i] = new _Column(i, name, fieldId, tableColNo, fieldType, dataSize, typeModifier, formatCode);
    }

    _query._columnCount = count;
    _query._columns = list;
    _query._commandIndex++;

    _query.addRowDescription();
  }

  void _readDataRow(int msgType, int length) {

    assert(_buffer.bytesAvailable >= length);

    int columns = _buffer.readInt16();
    for (var i = 0; i < columns; i++) {
      int size = _buffer.readInt32();
      _readColumnData(i, size);
    }
  }

  void _readColumnData(int index, int colSize) {

    assert(_buffer.bytesAvailable >= colSize);

    if (index == 0)
      _query._rowData = new List<dynamic>(_query._columns.length);

    if (colSize == -1) {
      _query._rowData[index] = null;
    } else {
      //TODO Optimisation. Don't always need to copy this data. Can read directly
      // out of the buffer.
      var col = _query._columns[index];
      var data = _buffer.readBytes(colSize);
      var value = (col.isBinary) ? _decodeBinaryValue(col, data)
                                  : _decodeStringValue(col, data);
      _query._rowData[index] = value;
    }

    // If last column, then return the row.
    if (index == _query._columnCount - 1)
      _query.addRow();
  }

  dynamic _decodeStringValue(_Column col, List<int> data) {

    switch (col.fieldType) {
      case _PG_BOOL:
        return data[0] == 116;

      case _PG_INT2:
      case _PG_INT4:
      case _PG_INT8:
        return int.parse(UTF8.decode(data));

      case _PG_FLOAT4:
      case _PG_FLOAT8:
        return double.parse(UTF8.decode(data));

      case _PG_TIMESTAMP:
      case _PG_DATE:
        return DateTime.parse(UTF8.decode(data));

      // Not implemented yet - return a string.
      case _PG_MONEY:
      case _PG_TIMESTAMPZ:
      case _PG_TIMETZ:
      case _PG_TIME:
      case _PG_INTERVAL:
      case _PG_NUMERIC:

      default:
        // Return a string for unknown types. The end user can parse this.
        return UTF8.decode(data);
    }
  }

  dynamic _decodeBinaryValue(_Column col, List<int> data) {
    // Not implemented
    return data;
  }

  void _readCommandComplete(int msgType, int length) {

    assert(_buffer.bytesAvailable >= length);

    var commandString = _buffer.readString(length);
    int rowsAffected =
        int.parse(commandString.split(' ').last, onError: (_) => null);

    _query._commandIndex++;
    _query._rowsAffected = rowsAffected;
  }

  void close() {
    if (state == _State.CLOSED)
      return;

    var prior = state;
    state = _State.CLOSED;

    try {
      var msg = new _MessageBuffer();
      msg.addByte(_MSG_TERMINATE);
      msg.addInt32(0);
      msg.setLength();

      _socket.add(msg.buffer);
    } catch (e) {
      _unhandled.add(new _PgClientException('Postgresql connection closed without sending terminate message.', e));
    }

    if (prior != _State.CLOSED)
      _closed.complete(null);

    _destroy();
  }

  void _destroy() {
    state = _State.CLOSED;
    _socket.destroy();
  }

  Future get onClosed => _closed.future;

  @override
  Future<List<Result>> executeMulti(String sql, [values]) {
    // TODO: implement executeMulti
  }
}
