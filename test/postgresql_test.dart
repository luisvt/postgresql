library postgresql_test;

import 'dart:async';
import 'package:ddbc/ddbc.dart';
import 'package:postgresql/postgresql.dart';
import 'package:unittest/unittest.dart';
import 'connection_settings.dart';

Future<PgConnection> connect() => new PgConnection(USER_NAME, PASSWORD, DB_NAME).connect();

main() {

  group('Connect', () {

    test('Connect', () {
      new PgConnection(USER_NAME, PASSWORD, DB_NAME).connect()
        .then(expectAsync((PgConnection c) {
          c.close();
        }));
    });

    test('Connect failure - incorrect password', () {
      new PgConnection(USER_NAME, 'WRONG', DB_NAME).connect()
        .then((c) => throw new Exception('Should not be reached.'),
          onError: expectAsync((err) { /* boom! */ }));
    });

    //Should fail with a message like:settings.toUri()
    //AsyncError: 'SocketIOException: OS Error: Connection refused, errno = 111'
    test('Connect failure - incorrect port', () {

      new PgConnection(USER_NAME, PASSWORD, DB_NAME, port: 9037).connect().then((c) => throw new Exception('Should not be reached.'),
        onError: expectAsync((err) { /* boom! */ }));
    });

//    test('Connect failure - connect to http server', () {
//      var uri = 'postgresql://user:pwd@google.com:80/database';
//      connect(uri).then((c) => throw new Exception('Should not be reached.'),
//        onError: expectAsync((err) { /* boom! */ }));
//    });

  });

  group('Close', () {
    test('Close multiple times.', () {
      connect().then((conn) {
        conn.close();
        conn.close();
        new Future.delayed(new Duration(milliseconds: 20))
          .then((_) { conn.close(); });
      });
    });

    test('Query on closed connection.', () {
      var cb = expectAsync((Exception e) {
        print(e);
      });
      connect().then((conn) {
        conn.close();
        conn.execute("select 'blah'")
          .then((_) =>
              throw new Exception('Should not be reached.'))
          .catchError(cb);
      });
    });

    test('Execute on closed connection.', () {
      var cb = expectAsync((e) {});
      connect().then((conn) {
        conn.close();
        conn.execute("select 'blah'")
          .then((_) => throw new Exception('Should not be reached.'))
          .catchError(cb);
      });
    });

  });

  group('Query >', () {

    PgConnection conn;

    setUp(() {
      return connect().then((c) => conn = c);
    });

    tearDown(() {
      if (conn != null) conn.close();
    });

    test('Invalid sql statement', () {
      conn.execute('elect 1').then(
          (result) => throw new Exception('Should not be reached.'),
          onError: expectAsync((err) { print(err); }));
    });

    test('Null sql statement', () {
      conn.execute(null).then(
          (result) => throw new Exception('Should not be reached.'),
          onError: expectAsync((err) { 
            print(err); 
            }));
    });

    test('Empty sql statement', () {
      conn.execute('').then(
          (result) => throw new Exception('Should not be reached.'),
          onError: expectAsync((err) { print(err); }));
    });

    test('Whitespace only sql statement', () {
      conn.execute('  ').then(
          expectAsync((Result result) => expect(result.rows.length, 0)),
          onError: (err) { throw new Exception('Should not be reached.'); });
    });

    test('Empty multi-statement', () {
      conn.execute('''
        select 'bob';
        ;
        select 'jim';
      ''').then(
          expectAsync((result) => expect(result.rows.length, 2)),
          onError: (err) { throw new Exception('Should not be reached.'); });
    });

    test('Query queueing', () {

      conn.execute('select 1').then(
          expectAsync((result) {
            expect(result.rows[0][0], equals(1));
          })
      );

      conn.execute('select 2').then(
          expectAsync((result) {
            expect(result.rows[0][0], equals(2));
          })
      );

      conn.execute('select 3').then(
          expectAsync((result) {
            expect(result.rows[0][0], equals(3));
          })
      );
    });

    test('Query queueing with error', () {

      conn.execute('elect 1').then(
          (result) => throw new Exception('Should not be reached.'),
          onError: expectAsync((err) { /* boom! */ }));

      conn.execute('select 2').then(
          expectAsync((result) {
            expect(result.rows[0][0], equals(2));
          })
      );

      conn.execute('select 3').then(
          expectAsync((result) {
            expect(result.rows[0][0], equals(3));
          })
      );
    });

    test('Multiple queries in a single sql statement', () {
      conn.execute('select 1; select 2; select 3;').then(
        expectAsync((result) {
          expect(result.rows[0][0], equals(1));
          expect(result.rows[1][0], equals(2));
          expect(result.rows[2][0], equals(3));
        })
      );
    });
  });

  group('Data types', () {

    Connection conn;

    setUp(() {
      return connect().then((c) => conn = c);
    });

    tearDown(() {
      if (conn != null) conn.close();
    });

    test('Select String', () {
      conn.execute("select 'blah'").then(
        expectAsync((result) => expect(result.rows[0][0], equals('blah')))
      );
    });

    test('Select UTF8 String', () {
      conn.execute("select '☺'").then(
        expectAsync((result) => expect(result.rows[0][0], equals('☺')))
      );
    });

    test('Select int', () {
      conn.execute('select 1, -1').then(
        expectAsync((result) {
          expect(result.rows[0][0], equals(1));
          expect(result.rows[0][1], equals(-1));
        })
      );
    });

    //FIXME Decimals not implemented yet.
    test('Select number', () {
      conn.execute('select 1.1').then(
        expectAsync((result) => expect(result.rows[0][0], equals('1.1')))
      );
    });

    test('Select boolean', () {
      conn.execute('select true, false').then(
        expectAsync((result) {
          expect(result.rows[0][0], equals(true));
          expect(result.rows[0][1], equals(false));
        })
      );
    });

    test('Select null', () {
      conn.execute('select null').then(
        expectAsync((result) {
          expect(result.rows[0][0], equals(null));
        })
      );
    });

    test('Select int 2', () {

      conn.execute('create temporary table dart_unit_test (a int2, b int4, c int8)');
      conn.execute('insert into dart_unit_test values (1, 2, 3)');
      conn.execute('insert into dart_unit_test values (-1, -2, -3)');
      conn.execute('insert into dart_unit_test values (null, null, null)');

      conn.execute('select a, b, c from dart_unit_test').then(
        expectAsync((result) {
          expect(result.rows[0][0], equals(1));
          expect(result.rows[0][1], equals(2));
          expect(result.rows[0][2], equals(3));

          expect(result.rows[1][0], equals(-1));
          expect(result.rows[1][1], equals(-2));
          expect(result.rows[1][2], equals(-3));

          expect(result.rows[2][0], equals(null));
          expect(result.rows[2][1], equals(null));
          expect(result.rows[2][2], equals(null));
        })
      );
    });

    test('Select timestamp', () {

      conn.execute('create temporary table dart_unit_test (a timestamp)');
      conn.execute("insert into dart_unit_test values ('1979-12-20 09:00')");

      conn.execute('select a from dart_unit_test').then(
        expectAsync((result) {
          expect(result.rows[0][0], equals(new DateTime(1979, 12, 20, 9)));
        })
      );
    });

    test('Select timestamp with milliseconds', () {
      var t0 = new DateTime(1979, 12, 20, 9, 0, 0);
      var t1 = new DateTime(1979, 12, 20, 9, 0, 9);
      var t2 = new DateTime(1979, 12, 20, 9, 0, 99);
      var t3 = new DateTime(1979, 12, 20, 9, 0, 999);

      conn.execute('create temporary table dart_unit_test (a timestamp)');

      var insert = 'insert into dart_unit_test values (@time)';
      conn.execute(insert, {"time": t0});
      conn.execute(insert, {"time": t1});
      conn.execute(insert, {"time": t2});
      conn.execute(insert, {"time": t3});

      conn.execute('select a from dart_unit_test').then(
        expectAsync((result) {
          expect(result.rows[0][0], equals(t0));
          expect(result.rows[1][0], equals(t1));
          expect(result.rows[2][0], equals(t2));
          expect(result.rows[3][0], equals(t3));
        })
      );
    });

    test('Select DateTime', () {

      conn.execute('create temporary table dart_unit_test (a date)');
      conn.execute("insert into dart_unit_test values ('1979-12-20')");

      conn.execute('select a from dart_unit_test').then(
        expectAsync((result) {
          expect(result.rows[0][0], equals(new DateTime(1979, 12, 20)));
        })
      );
    });

    test('Select double', () {

      conn.execute('create temporary table dart_unit_test (a float4, b float8)');
      conn.execute("insert into dart_unit_test values (1.1, 2.2)");

      conn.execute('select a, b from dart_unit_test').then(
        expectAsync((result) {
          expect(result.rows[0][0], equals(1.1.toDouble()));
          expect(result.rows[0][1], equals(2.2.toDouble()));
        })
      );
    });

    //TODO
    // numeric (Need a BigDecimal type).
    // time
    // interval
    // timestamp and date with a timezone offset.

  });

  group('Execute', () {

    Connection conn;

    setUp(() {
      return connect().then((c) => conn = c);
    });

    tearDown(() {
      if (conn != null) conn.close();
    });

    test('Rows affected', () {
      conn.execute('create temporary table dart_unit_test (a int)');

      conn.execute('insert into dart_unit_test values (1), (2), (3)').then(
          expectAsync((Result result) {
            expect(result.affectedRows, equals(3));
          })
      );

      conn.execute('update dart_unit_test set a = 5 where a = 1').then(
          expectAsync((result) {
            expect(result.affectedRows, equals(1));
          })
      );

      conn.execute('delete from dart_unit_test where a > 2').then(
          expectAsync((result) {
            expect(result.affectedRows, equals(2));
          })
      );

      conn.execute('create temporary table bob (a int)').then(
          expectAsync((result) {
            expect(result.affectedRows, equals(null));
          })
      );

      conn.execute('''
        select 'one';
        create temporary table jim (a int);
        create temporary table sally (a int);
      ''').then(
          expectAsync((result) {
            expect(result.affectedRows, equals(null));
          })
      );

    });

  });

  group('PgException', () {

    Connection conn;

    setUp(() {
      return connect().then((c) => conn = c);
    });

    tearDown(() {
      if (conn != null) conn.close();
    });

    // This test depends on the locale settings of the postgresql server.
    test('Error information for invalid sql statement', () {
      conn.execute('elect 1').then(
          (result) => throw new Exception('Should not be reached.'),
          onError: expectAsync((err) {
            expect(err.severity, equals('ERROR'));
            expect(err.code, equals('42601'));
            expect(err.position, equals(1));
          }));
    });
  });

  group('Object mapping', () {

    Connection conn;

    setUp(() {
      return connect().then((c) => conn = c);
    });

    tearDown(() {
      if (conn != null) conn.close();
    });

    test('Map person.', () {
      conn.execute('''
        select 'Greg' as firstname, 'Lowe' as lastname;
        select 'Bob' as firstname, 'Jones' as lastname;
      ''')
        .then((result) => result.rows.map((row) => new Person()
                            ..firstname = row.firstname
                            ..lastname = row.lastname))
        
        .then(expectAsync((result) { }));
    });

    test('Map person immutable.', () {
      conn.execute('''
          select 'Greg' as firstname, 'Lowe' as lastname;
          select 'Bob' as firstname, 'Jones' as lastname;
      ''')
        .then((result) => result.rows.map((row) => new ImmutablePerson(row.firstname, row.lastname)))
        .then(expectAsync((result) { }));
    });
  });

  group('Transactions', () {

    PgConnection conn1;
    PgConnection conn2;

    setUp(() {
      return connect()
              .then((c) => conn1 = c)
              .then((_) => connect())
              .then((c) => conn2 = c)
              .then((_) => conn1.execute('create table if not exists tx (val int); delete from tx;')); // if not exists requires pg9.1
    });

    tearDown(() {
      if (conn1 != null) conn1.close();
      if (conn2 != null) conn2.close();
    });

    test('simple query', () {
      var cb = expectAsync((_) { });
      conn1.runInTransaction(() {
        return conn1.execute("select 'oi'")
          .then((result) {
            print(result.rows[0]);
            expect(result.rows[0][0], equals('oi')); 
          });
      }).then(cb);
    });

    test('simple query read committed', () {
      var cb = expectAsync((_) { });
      conn1.runInTransaction(() {
        return conn1.execute("select 'oi'")
          .then((result) { expect(result.rows[0][0], equals('oi')); });
      }, Isolation.READ_COMMITTED).then(cb);
    });

    test('simple query repeatable read', () {
      var cb = expectAsync((_) { });
      conn1.runInTransaction(() {
        return conn1.execute("select 'oi'")
          .then((result) { expect(result.rows[0][0], equals('oi')); });
      }, Isolation.REPEATABLE_READ).then(cb);
    });

    test('simple query serializable', () {
      var cb = expectAsync((_) { });
      conn1.runInTransaction(() {
        return conn1.execute("select 'oi'")
          .then((result) { expect(result.rows[0][0], equals('oi')); });
      }, Isolation.SERIALIZABLE).then(cb);
    });


    test('rollback', () {
      var cb = expectAsync((_) { });

      conn1.runInTransaction(() {
        return conn1.execute('insert into tx values (42)')
          .then((_) => conn1.execute('select val from tx'))
          .then((result) { expect(result.rows[0][0], equals(42)); })
          .then((_) => throw new Exception('Boom!'));
      })
      .catchError((e) => print('Ignore: $e'))
      .then((_) => conn1.execute('select val from tx'))
      .then((result) { 
        expect(result.rows, equals([]));
      })
      .then(cb);
    });

/*
    test('isolation', () {
      var cb = expectAsync((_) { });
      var cb2 = expectAsync((_) { });

      print('isolation');

      conn1.runInTransaction(() {
        return conn1.execute('insert into tx values (42)')
          .then((_) => conn1.query('select val from tx'))
          .then((result) { expect(result.rows[0][0], equals(42)); });
      })
      .then((_) => conn1.query('select val from tx'))
      .then((result) { expect(result.rows[0][0], equals(42)); })
      .then(cb);

      conn2.runInTransaction(() {
        return conn1.execute('insert into tx values (43)')
          .then((_) => conn1.query('select val from tx'))
          .then((result) { expect(result.rows[0][0], equals(43)); });
      })
      .then((_) => conn1.query('select val from tx'))
      .then((result) { expect(result.rows[0][0], equals(43)); })
      .then(cb2);
    });
*/

  });

}


class Person {
  String firstname;
  String lastname;
  String toString() => '$firstname $lastname';
}

class ImmutablePerson {
  ImmutablePerson(this.firstname, this.lastname);
  final String firstname;
  final String lastname;
  String toString() => '$firstname $lastname';
}