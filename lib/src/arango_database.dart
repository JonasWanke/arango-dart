import 'dart:convert';

import 'package:arango/src/arango_cursor.dart';
import 'package:arango/src/arango_query.dart';
import 'package:arango/src/arango_transaction.dart';
import 'package:arango/src/collection/arango_collection.dart';
import 'package:arango/src/arango_config.dart';
import 'package:arango/src/arango_connection.dart';
import 'package:arango/src/arango_errors.dart';
import 'package:arango/src/arango_helper.dart';
import 'package:arango/src/collection/arango_document_collection.dart';
import 'package:arango/src/collection/arango_edge_collection.dart';
import 'package:arango/src/graph/arango_graph.dart';

ArangoCollection _constructCollection(
  ArangoConnection connection,
  Map<String, dynamic> data,
) {
  final name = data['name'];
  return data['type'] == CollectionType.edgeCollection
      ? ArangoEdgeCollection(name, connection)
      : ArangoDocumentCollection(name, connection);
}

class CreateDatabaseUser {
  CreateDatabaseUser(
    this.username, {
    this.passwd,
    this.active,
    this.extra,
  });

  final String username;
  final String? passwd;
  final bool? active;
  final Map<String, dynamic>? extra;

  Map<String, dynamic> toJson() => {
        'username': username,
        'passwd': passwd,
        'active': active,
        'extra': extra,
      };
}

class ArangoDatabase {
  late final ArangoConnection _connection;

  ArangoDatabase(String url) {
    final config = ArangoConfig(url: url);
    _connection = ArangoConnection(config);
  }

  //#region misc
  Future<Map<String, dynamic>> version() async {
    final resp = await _connection.request(
      method: 'GET',
      path: '/_api/version',
    );
    return resp.body;
  }
  //#endregion

  //#region auth
  Future login([String username = 'root', String password = '']) async {
    final resp = await _connection.request(
      method: 'POST',
      path: '/_open/auth',
      body: {'username': username, 'password': password},
    );
    useBearerAuth(resp.body['jwt']);
    return resp.body;
  }

  ArangoDatabase useBasicAuth(String username, String password) {
    final bytes = utf8.encode('$username:$password');
    final basic = base64.encode(bytes);
    _connection.setHeader('authorization', 'Basic $basic');
    return this;
  }

  ArangoDatabase useBearerAuth(String? token) {
    _connection.setHeader('authorization', 'Bearer $token');
    return this;
  }
  //#endregion

  //#region databases
  ArangoDatabase useDatabase(String name) {
    _connection.setDatabaseName(name);
    return this;
  }

  Future<Map<String, dynamic>> current() async {
    final resp = await _connection.request(
      path: '/_api/database/current',
    );
    return asJson(resp.body['result']);
  }

  Future<bool> exists() async {
    try {
      await current();
      return true;
    } on ArangoError catch (e) {
      const databaseNotFound = 1228;
      if (e.errorNum == databaseNotFound) {
        return false;
      } else {
        rethrow;
      }
    }
  }

  Future<void> createDatabase(
    String databaseName, [
    List<CreateDatabaseUser>? users,
  ]) {
    final userList = users?.map((u) => u.toJson()).toList();
    return _connection.request(
      method: 'POST',
      path: '/_api/database',
      body: {'name': databaseName, 'users': userList},
    );
  }

  Future<List<String>> listDatabases() async {
    final resp = await _connection.request(
      path: '/_api/database',
    );
    return List<String>.from(resp.body['result']);
  }

  Future<List<String>> listUserDatabases() async {
    final resp = await _connection.request(
      path: '/_api/database/user',
    );
    return List<String>.from(resp.body['result']);
  }

  Future<void> dropDatabase(String databaseName) {
    return _connection.request(
      method: 'DELETE',
      path: '/_api/database/$databaseName',
    );
  }
  //#endregion

  //#region collections
  ArangoDocumentCollection collection(String collectionName) {
    return ArangoDocumentCollection(collectionName, _connection);
  }

  ArangoEdgeCollection edgeCollection(String collectionName) {
    return ArangoEdgeCollection(collectionName, _connection);
  }

  Future<List<Map<String, dynamic>>> listCollections({
    bool excludeSystem = true,
  }) async {
    final resp = await _connection.request(
        path: '/_api/collection',
        queries: {'excludeSystem': excludeSystem.toString()});

    final data = _connection.arangoMajor <= 2
        ? resp.body['collections']
        : resp.body['result'];

    return List<Map<String, dynamic>>.from(data);
  }

  Future<List<ArangoCollection>> collections({
    bool excludeSystem = true,
  }) async {
    final collections = await listCollections(excludeSystem: excludeSystem);
    return collections
        .map((data) => _constructCollection(_connection, data))
        .toList();
  }

  Future<void> truncate({bool excludeSystem = true}) async {
    final collections = await listCollections(excludeSystem: excludeSystem);
    final futures = collections.map((coll) {
      return _connection.request(
        method: 'PUT',
        path: '/_api/collection/${coll['name']}/truncate',
      );
    });
    await Future.wait(futures);
  }
  //#endregion

  ArangoQuery query() {
    return ArangoQuery(this);
  }

  Future<ArangoCursor> rawQuery(
    String aql, {
    bool returnCount = false,
    bool allowDirtyRead = false,
    Map<String, dynamic>? bindVars,
    Duration? timeout,
    int? batchSize,
    int? ttl,
    bool? cache,
    int? memoryLimit,
    Map<String, dynamic>? options,
  }) async {
    final resp = await _connection.request(
      method: 'POST',
      path: '/_api/cursor',
      allowDirtyRead: allowDirtyRead,
      timeout: timeout,
      body: {
        'query': aql,
        'count': returnCount,
        if (bindVars != null) 'bindVars': bindVars,
        if (batchSize != null) 'batchSize': batchSize,
        if (ttl != null) 'ttl': ttl,
        if (cache != null) 'cache': cache,
        if (memoryLimit != null) 'memoryLimit': memoryLimit,
        if (options != null) 'options': options,
      },
    );

    return ArangoCursor(
      _connection,
      resp.arangoDartHostId,
      allowDirtyRead,
      resp.body,
    );
  }
  //#endregion

  //#region transaction

  Future<dynamic> executeTransaction({
    required List<String> write,
    required String action,
    List<String>? exclusive,
    List<String>? read,
    dynamic params,
    bool? allowImplicit,
    int? lockTimeout,
    int? maxTransactionSize,
    bool? waitForSync,
  }) async {
    final resp = await _connection.request(
      method: 'POST',
      path: '/_api/transaction',
      body: {
        'collections': {
          'write': write,
          if (exclusive != null) 'exclusive': exclusive,
          if (read != null) 'read': read,
        },
        'action': action,
        if (params != null) 'params': params,
        if (allowImplicit != null) 'allowImplicit': allowImplicit,
        if (lockTimeout != null) 'lockTimeout': lockTimeout,
        if (maxTransactionSize != null)
          'maxTransactionSize': maxTransactionSize,
        if (waitForSync != null) 'waitForSync': waitForSync,
      },
    );
    return resp.body['result'];
  }

  Future<ArangoTransaction> beginTransaction({
    List<String>? exclusive,
    List<String>? write,
    List<String>? read,
    bool? allowImplicit,
    int? lockTimeout,
    int? maxTransactionSize,
    bool? waitForSync,
  }) async {
    final resp = await _connection.request(
      method: 'POST',
      path: '/_api/transaction/begin',
      body: {
        'collections': {
          if (exclusive != null) 'exclusive': exclusive,
          if (write != null) 'write': write,
          if (read != null) 'read': read,
        },
        if (allowImplicit != null) 'allowImplicit': allowImplicit,
        if (lockTimeout != null) 'lockTimeout': lockTimeout,
        if (maxTransactionSize != null)
          'maxTransactionSize': maxTransactionSize,
        if (waitForSync != null) 'waitForSync': waitForSync,
      },
    );
    return ArangoTransaction(_connection, resp.body['result']['id']);
  }

  Future<List<Map<String, dynamic>>> listTransactions() async {
    final resp = await _connection.request(path: '/_api/transaction');
    return List<Map<String, dynamic>>.from(resp.body['transactions']);
  }

  Future<List<ArangoTransaction>> transactions() async {
    final transactions = await listTransactions();
    return transactions
        .map((t) => ArangoTransaction(_connection, t['id']))
        .toList();
  }
  //#endregion

  //#region collections
  ArangoGraph graph(String name) {
    assert(_connection.arangoMajor >= 3);

    return ArangoGraph(name, _connection);
  }

  Future<List<Map<String, dynamic>>> listGraphs() async {
    assert(_connection.arangoMajor >= 3);

    final response = await _connection.request(path: '/_api/gharial');
    return (response.body['graphs'] as List<dynamic>)
        .cast<Map<String, dynamic>>()
        .toList();
  }

  Future<List<ArangoGraph>> graphs() async {
    assert(_connection.arangoMajor >= 3);

    final graphs = await listGraphs();
    return graphs
        .map((data) => ArangoGraph(data['name'] as String, _connection))
        .toList();
  }
  //#endregion
}
