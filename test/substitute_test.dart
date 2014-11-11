library postgresql_test;

import 'package:ddbc/ddbc.dart';
import 'package:unittest/unittest.dart';
import 'package:postgresql/postgresql.dart';
import 'connection_settings.dart';

main() {
  
  var sf = new PgSqlFormatter();

  test('Format value', () {
    expect(sf.format('bob', null), equals("E'bob'"));
    expect(sf.format('bo\nb', null), equals(r"E'bo\nb'"));
    expect(sf.format('bo\rb', null), equals(r"E'bo\rb'"));
    expect(sf.format(r'bo\b', null), equals(r"E'bo\\b'"));

    expect(sf.format(r"'", null), equals(r"E'\''"));
    expect(sf.format(r" '' ", null), equals(r"E' \'\' '"));
    expect(sf.format(r"\''", null), equals(r"E'\\\'\''"));
  });


  group('Query', () {

    Connection conn = new PgConnection(USER_NAME, PASSWORD, DB_NAME);
    
    tearDown(() {
      if (conn != null) conn.close();
    });

    test('Substitution', () {
      conn.execute(
          'select @num, @num:string, @num:number, '
          '@int, @int:string, @int:number, '
          '@string, '
          '@datetime, @datetime:date, @datetime:timestamp, '
          '@boolean, @boolean_false, @boolean_null',
          { 'num': 1.2,
            'int': 3,
            'string': 'bob\njim',
            'datetime': new DateTime(2013, 1, 1),
            'boolean' : true,
            'boolean_false' : false,
            'boolean_null' : null,
          })
            .then(expectAsync1((Result result) { 
        print(result.rows);
        
      }));
      });
  });

}