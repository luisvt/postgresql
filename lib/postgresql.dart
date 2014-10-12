library postgresql;

import 'dart:async';
import 'dart:collection';
import 'package:crypto/crypto.dart';
import 'dart:io';
import 'dart:convert';
import 'package:enums/enums.dart';
import 'package:ddbc/ddbc.dart';

part 'buffer.dart';
part 'connection.dart';
part 'constants.dart';
part 'exceptions.dart';
part 'format_value.dart';
part 'message_buffer.dart';
part 'query.dart';
part 'settings.dart';
part 'substitute.dart';
part 'postgresql_pool.dart';

class Transaction extends Enum {
  final int value;
  const Transaction._(this.value);

  static const TRANSACTION_UNKNOWN = const Transaction._(1);
  static const TRANSACTION_NONE = const Transaction._(2);
  static const TRANSACTION_BEGUN = const Transaction._(3);
  static const TRANSACTION_ERROR = const Transaction._(4);
}

class Isolation extends Enum {
  final int value;
  const Isolation(this.value);

  static const Isolation READ_COMMITTED = const Isolation(1);
  static const Isolation REPEATABLE_READ = const Isolation(2);
  static const Isolation SERIALIZABLE = const Isolation(3);
  
}

/// Made public for testing.
String substitute(String source, values) => _substitute(source, values);
String formatValue(value, String type) => _formatValue(value, type);
