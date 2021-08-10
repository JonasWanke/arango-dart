import 'package:arango/src/arango_connection.dart';
import 'package:arango/src/graph/arango_graph.dart';

enum ArangoGraphCollectionType { vertex, edge }

abstract class ArangoGraphCollection {
  final ArangoGraph graph;
  final String name;
  final ArangoGraphCollectionType type;

  String get _basePath {
    final String typeString;
    switch (type) {
      case ArangoGraphCollectionType.vertex:
        typeString = 'vertex';
        break;
      case ArangoGraphCollectionType.edge:
        typeString = 'edge';
        break;
    }
    return '/_api/gharial/${graph.name}/$typeString/$name';
  }

  final ArangoConnection _connection;

  ArangoGraphCollection(this.graph, this.type, this.name, this._connection);

  Future<Map<String, dynamic>> create(
    Map<String, dynamic> data, {
    bool? waitForSync,
    bool? returnNew,
  }) async {
    final response = await _connection.request(
      method: 'POST',
      path: '$_basePath',
      queries: {
        if (waitForSync != null) 'waitForSync': waitForSync.toString(),
        if (returnNew != null) 'returnNew': returnNew.toString(),
        'collection': name,
      },
      body: data,
    );
    return response.body;
  }

  Future<Map<String, dynamic>> get(
    String key, {
    String? rev,
    String? ifMatch,
    String? ifNoneMatch,
  }) async {
    final response = await _connection.request(
      path: '$_basePath/$key',
      queries: {if (rev != null) 'rev': rev},
      headers: {
        if (ifMatch != null) 'If-Match': ifMatch,
        if (ifNoneMatch != null) 'If-None-Match': ifNoneMatch,
      },
    );
    return response.body['vertex'] as Map<String, dynamic>;
  }

  Future<Map<String, dynamic>> update(
    String key,
    Map<String, dynamic> data, {
    bool? waitForSync,
    bool? keepNull,
    bool? returnOld,
    bool? returnNew,
    String? ifMatch,
  }) async {
    final response = await _connection.request(
      method: 'PATCH',
      path: '$_basePath/$key',
      queries: {
        if (waitForSync != null) 'waitForSync': waitForSync.toString(),
        if (keepNull != null) 'keepNull': keepNull.toString(),
        if (returnOld != null) 'returnOld': returnOld.toString(),
        if (returnNew != null) 'returnNew': returnNew.toString(),
      },
      headers: {if (ifMatch != null) 'If-Match': ifMatch},
    );
    return response.body;
  }

  Future<Map<String, dynamic>> replace(
    String key,
    Map<String, dynamic> data, {
    bool? waitForSync,
    bool? keepNull,
    bool? returnOld,
    bool? returnNew,
    String? ifMatch,
  }) async {
    final response = await _connection.request(
      method: 'PUT',
      path: '$_basePath/$key',
      queries: {
        if (waitForSync != null) 'waitForSync': waitForSync.toString(),
        if (keepNull != null) 'keepNull': keepNull.toString(),
        if (returnOld != null) 'returnOld': returnOld.toString(),
        if (returnNew != null) 'returnNew': returnNew.toString(),
      },
      headers: {if (ifMatch != null) 'If-Match': ifMatch},
    );
    return response.body;
  }

  Future<Map<String, dynamic>> remove(
    String key, {
    bool? waitForSync,
    bool? returnOld,
    String? ifMatch,
  }) async {
    final response = await _connection.request(
      method: 'DELETE',
      path: '$_basePath/$key',
      queries: {
        if (waitForSync != null) 'waitForSync': waitForSync.toString(),
        if (returnOld != null) 'returnOld': returnOld.toString(),
      },
      headers: {if (ifMatch != null) 'If-Match': ifMatch},
    );
    return response.body;
  }
}

class ArangoGraphVertexCollection extends ArangoGraphCollection {
  ArangoGraphVertexCollection(
    ArangoGraph graph,
    String name,
    ArangoConnection connection,
  ) : super(graph, ArangoGraphCollectionType.vertex, name, connection);
}

class ArangoGraphEdgeCollection extends ArangoGraphCollection {
  ArangoGraphEdgeCollection(
    ArangoGraph graph,
    String name,
    ArangoConnection connection,
  ) : super(graph, ArangoGraphCollectionType.edge, name, connection);
}
