#!/bin/bash

# Abort if non-zero code returned.
set -e

dartanalyzer lib/postgresql.dart
dartanalyzer lib/postgresql_pool.dart
dartanalyzer test/postgresql_test.dart
dartanalyzer test/postgresql_pool_test.dart
dartanalyzer test/substitute_test.dart

dart --checked test/settings_test.dart
dart --checked test/postgresql_test.dart
dart --checked test/postgresql_pool_test.dart
dart --checked test/substitute_test.dart