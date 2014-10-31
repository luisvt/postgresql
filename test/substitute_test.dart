library postgresql_test;

import 'package:ddbc/ddbc.dart';
import 'package:unittest/unittest.dart';
import 'package:postgresql/postgresql.dart';
import 'connection_settings.dart';

main() {
//
//  group('Substitute by id', () {
//    test('Substitute 1', () {
//      var result = substitute('@id', {'id': 20});
//      expect(result, equals('20'));
//    });
//
//    test('Substitute 2', () {
//      var result = substitute('@id ', {'id': 20});
//      expect(result, equals('20 '));
//    });
//
//    test('Substitute 3', () {
//      var result = substitute(' @id ', {'id': 20});
//      expect(result, equals(' 20 '));
//    });
//
//    test('Substitute 4', () {
//      var result = substitute('@id@bob', {'id': 20, 'bob': 13});
//      expect(result, equals('2013'));
//    });
//
//    test('Substitute 5', () {
//      var result = substitute('..@id..', {'id': 20});
//      expect(result, equals('..20..'));
//    });
//
//    test('Substitute 6', () {
//      var result = substitute('...@id...', {'id': 20});
//      expect(result, equals('...20...'));
//    });
//
//    test('Substitute 7', () {
//      var result = substitute('...@id.@bob...', {'id': 20, 'bob': 13});
//      expect(result, equals('...20.13...'));
//    });
//
//    test('Substitute 8', () {
//      var result = substitute('...@id@bob', {'id': 20, 'bob': 13});
//      expect(result, equals('...2013'));
//    });
//
//    test('Substitute 9', () {
//      var result = substitute('@id@bob...', {'id': 20, 'bob': 13});
//      expect(result, equals('2013...'));
//    });
//
//    test('Substitute 10', () {
//      var result = substitute('@id:string', {'id': 20, 'bob': 13});
//      expect(result, equals("'20'"));
//    });
//
//    test('Substitute 11', () {
//      var result = substitute('@blah_blah', {'blah_blah': 20});
//      expect(result, equals("20"));
//    });
//
//    test('Substitute 12', () {
//      var result = substitute('@_blah_blah', {'_blah_blah': 20});
//      expect(result, equals("20"));
//    });
//  });

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