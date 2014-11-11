library postgresql_test;

import 'dart:async';
import 'dart:io';
import 'package:unittest/unittest.dart';
import 'package:postgresql/postgresql.dart';
import 'connection_settings.dart';


main() {
  
  PgConnectionPool pool = new PgConnectionPool(USER_NAME, PASSWORD, DB_NAME);
  int tout = 2 * 1000; // Should be longer than usage
  
  
  test('Connect', () {
  	var pass = expectAsync(() {});

    testConnect(_) {
    	pool.execute("select 'oi';")
    			.then((_) => 
              print('fast query done.'))
      .catchError((err) => print('Connect error: $err'));
    }

    slowQuery() {
     pool.execute("select generate_series (1, 10000);")
          .then((_) => 
              print('slow query done.'))
//          .then((_) => conn.close())
          .catchError((err) => print('Query error: $err'));
//      .catchError((err) => print('Connect error: $err')); 
    }

    // Wait for initial connections to be made before starting
    Timer timer;
    pool.start().then((_) {
      for (var i = 0; i < 10; i++)
        slowQuery();
//        testConnect();

      timer = new Timer.periodic(new Duration(milliseconds: 100), testConnect);
    });

    new Future.delayed(new Duration(seconds: 1), () {
      timer.cancel();
      pool.destroy();
      print('Pool destroyed.');
      pass();
      exit(0); //FIXME - something is keeping the process alive.
    });

  });
}