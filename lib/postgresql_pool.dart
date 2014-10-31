part of postgresql;

//TODO implement lifetime. When connection is release if been open for more than lifetime millis, then close the connection, and open another. But need some way to stagger, initial creation, so they don't all expire at the same time.

class PgConnectionPool extends ConnectionPool {
	final String _uri;
	final _connections = new List<_PoolConnection>();
	final _available = new List<_PoolConnection>();
	final _waitingForRelease = new Queue<Completer>();
	bool _destroyed = false;
	int get _count => _connections.length + _connecting;
	int _connecting = 0;

	PgConnectionPool(
      String userName,
      String password,
      String dbName,
      {
        String host: '127.0.0.1',
        int port: 5432,
        int min: 2,
        int max: 5,
        int maxPacketSize: 16 * 1024 * 1024,
//      bool useCompression: false,
        bool useSSL: false,
        int timeout
      }) :
        _uri = 'postgres://$userName:$password@$host:$port/$dbName${useSSL?'?sslmode=require':''}',
        super(userName, password, dbName,
          host: host, port: port,
          min: min, max: max,
          maxPacketSize: maxPacketSize, useSSL: useSSL, timeout: timeout);

  @override
  Future<Result> execute(String sql, [parameters, bool transactional = false, bool consistent = true]) {
    return connect(timeout).then((conn) =>
      conn.execute(sql, parameters).then((result) {
        conn.close();
        return result;
      })
    );
  }

  @override
  Future<List<Result>> executeMulti(String sql, [parameters, bool transactional = false, bool consistent = true]) {
    // TODO: implement executeMulti
  }

	Future start() {
		var futures = new List<Future>(min);
		for (int i = 0; i < min; i++) {
			futures[i] = _incConnections();
		}
		return Future.wait(futures).then((_) { 
		  return true; 
		});
	}

	Future<_PoolConnection> connect([int timeout2]) {
		if (_destroyed)
			return new Future.error('Connect() called on destroyed pool.');

		if (!_available.isEmpty)
			return new Future.value(_available.removeAt(0));

		if (_count < max) {
			return _incConnections().then((_) {
				if (_available.isEmpty)
					throw new Exception('No connections available.'); //FIXME exception type.

				var c = _available.removeAt(0);

				if (_destroyed) {
					_destroy(c);
					throw new Exception('Connect() called on destroyed pool (Pool was destroyed during connection establishment).');
				}

				if (!c._isReleased) {
					throw new Exception('Connection not released.'); //FIXME
				}

				_setObtainedState(c, timeout2 == null ? timeout : timeout2);

				return c;
			});
		} else {
			return _whenConnectionReleased().then((_) {
				return connect(timeout2);
			});
		}
	}

	// Close all connections and cleanup.
	void destroy() {
		_destroyed = true;

		// Immediately close all connections
		for (var c in _connections)
			c._conn.close();

		_available.clear();
		_connections.clear();

		for (var c in _waitingForRelease)
			c.completeError('Connection pool destroyed.');
		_waitingForRelease.clear();
	}

	// Wait for a connection to be released.
	Future _whenConnectionReleased() {
		Completer completer = new Completer();
		_waitingForRelease.add(completer);
		return completer.future;
	}

	// Tell one listener that a connection has been released.
	void _connectionReleased() {
		if (!_waitingForRelease.isEmpty)
			_waitingForRelease.removeFirst().complete(null);
	}

	// Establish another connection, add to the list of available connections.
	Future _incConnections() {
		_connecting++;
		;
		return new PgConnection(user, password, dbName,
        host: host, port: port,
        maxPacketSize: maxPacketSize, useSSL: useSSL, timeout: timeout).connect()
		 .then((c) {
		 	var conn = new _PoolConnection(this, c);
		 	c.onClosed.then((_) => _handleUnexpectedClose(conn));
		 	_connections.add(conn);
		 	_available.add(conn);
		 	_connecting--;
		 })
		 .catchError((err) {
		 	_connecting--;
		 	//FIXME logging.
		 	print('Pool connect error: $err');
		 });
	}

	void _release(_PoolConnection conn) {
		if (_available.contains(conn)) {
			_connectionReleased();
			return;
		}

		if (conn.isClosed || conn.transactionStatus != Transaction.TRANSACTION_NONE) {

			//TODO lifetime expiry.
			//|| conn._obtained.millis > lifetime

			//TODO logging.
			print('Connection returned in bad transaction state: $conn.transactionStatus');
			_destroy(conn);
			_incConnections();
		
		} else {
			_setReleasedState(conn);
			_available.add(conn);
		}

		_connectionReleased();
	}

	void _destroy(_PoolConnection conn) {
		conn._conn.close();           // OK if already closed.
		_connections.remove(conn);
		_available.remove(conn);
	}

	void _setObtainedState(_PoolConnection conn, int timeout2) {
		conn._isReleased = false;
		conn._obtained = new DateTime.now();
		conn._timeout = timeout2;
		conn._reaper = null;

		if (timeout2 != null) {
			conn._reaper = new Timer(new Duration(milliseconds: timeout), () {
				print('Connection not released within timeout: ${conn._timeout}ms.'); //TODO logging.
				_destroy(conn);
			});
		}
	}

	void _setReleasedState(_PoolConnection conn) {
		conn._isReleased = true;
		conn._timeout = null;
		if (conn._reaper != null) {
			conn._reaper.cancel();
			conn._reaper = null;
		}
	}

	void _handleUnexpectedClose(_PoolConnection conn) {
		if (_destroyed) return; // Expect closes while destroying connections.

		print('Connection closed unexpectedly. Removed from pool.'); //TODO logging.
		_destroy(conn);

		// TODO consider automatically restarting lost connections.
	}

	toString() => 'Pool connecting: $_connecting available: ${_available.length} total: ${_connections.length}';

}

class _PoolConnection {
  
	final PgConnectionPool _pool;
	final PgConnection _conn;
	final DateTime _connected = new DateTime.now();
	DateTime _obtained;
	bool _isReleased = true;
	int _timeout;
	Timer _reaper; // Kills connections after a timeout expires.

	_PoolConnection(this._pool, Connection conn)
		: _conn = conn;

	void close() => _pool._release(this);
	Future<Result> execute(String sql, [values]) => _conn.execute(sql, values);

//	Future runInTransaction(Future operation(), [pg.Isolation isolation = pg.Isolation.READ_COMMITTED])
//		=> _conn.runInTransaction(operation, isolation);

	bool get isClosed => false; //TODO.
	Enum get transactionStatus => _conn.transactionStatus;

	Stream<dynamic> get unhandled { throw new UnimplementedError(); }

	Future get onClosed { throw new UnimplementedError(); }
}