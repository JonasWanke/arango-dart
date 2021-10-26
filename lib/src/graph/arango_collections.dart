import 'package:arango/src/arango_connection.dart';
import 'package:arango/src/arango_errors.dart';
import 'package:arango/src/collection/arango_document_collection.dart';
import 'package:arango/src/collection/arango_edge_collection.dart';
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

  ArangoGraphCollection(this.graph, this.type, this.name, this._connection)
      : assert(_connection.arangoMajor >= 3);

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

  @override
  bool operator ==(dynamic other) {
    return identical(this, other) ||
        (runtimeType == other.runtimeType &&
            other is ArangoGraphCollection &&
            graph == other.graph &&
            name == other.name &&
            type == other.type &&
            _connection == other._connection);
  }

  @override
  int get hashCode {
    return runtimeType.hashCode ^
        graph.hashCode ^
        name.hashCode ^
        type.hashCode ^
        _connection.hashCode;
  }
}

class ArangoGraphVertexCollection extends ArangoGraphCollection {
  ArangoGraphVertexCollection(
    ArangoGraph graph,
    String name,
    ArangoConnection connection,
  ) : super(graph, ArangoGraphCollectionType.vertex, name, connection);

  ArangoDocumentCollection get rawCollection =>
      ArangoDocumentCollection(name, _connection);

  Future<bool> doesCollectionExist() async {
    if (!(await graph.doesExist())) return false;
    return (await graph.listVertexCollections()).contains(this);
  }

  Future<Map<String, dynamic>> addCollectionToGraph() async {
    final response = await _connection.request(
      method: 'POST',
      path: '/_api/gharial/${graph.name}/vertex',
      body: {'collection': name},
    );
    return response.body;
  }

  Future<void> ensureCollectionExistsAndIsAddedToGraph() async {
    if (await doesCollectionExist()) return;

    await graph.ensureExists();
    await rawCollection.ensureExists();
    try {
      await addCollectionToGraph();
    } on ArangoError catch (e) {
      const graphCollectionUsedInOrphans = 1938;
      if (e.errorNum == graphCollectionUsedInOrphans) {
        return;
      } else {
        rethrow;
      }
    }
  }

  Future<Map<String, dynamic>> removeCollectionFromGraph({
    bool? dropCollection,
  }) async {
    final response = await _connection.request(
      method: 'DELETE',
      path: _basePath,
      queries: {
        if (dropCollection != null) 'dropCollection': dropCollection.toString(),
      },
    );
    return response.body;
  }
}

class ArangoGraphEdgeCollection extends ArangoGraphCollection {
  ArangoGraphEdgeCollection(
    ArangoGraph graph,
    String name,
    ArangoConnection connection,
  ) : super(graph, ArangoGraphCollectionType.edge, name, connection);

  ArangoEdgeCollection get rawCollection =>
      ArangoEdgeCollection(name, _connection);

  Future<bool> doesCollectionExist() async {
    if (!(await graph.doesExist())) return false;
    return (await graph.listEdgeCollections()).contains(this);
  }

  Future<Map<String, dynamic>> addCollectionToGraph(
    ArangoEdgeDefinition definition,
  ) async {
    assert(definition.collection == this);

    final response = await _connection.request(
      method: 'POST',
      path: '/_api/gharial/${graph.name}/edge',
      body: definition.toJson(),
    );
    return response.body;
  }

  Future<void> ensureCollectionExistsAndIsAddedToGraph(
    ArangoEdgeDefinition definition,
  ) async {
    if (await doesCollectionExist()) return;

    await graph.ensureExists();
    await rawCollection.ensureExists();
    try {
      await addCollectionToGraph(definition);
    } on ArangoError catch (e) {
      const graphInternalEdgeCollectionAlreadySet = 1942;
      if (e.errorNum == graphInternalEdgeCollectionAlreadySet) {
        return;
      } else {
        rethrow;
      }
    }
  }

  Future<Map<String, dynamic>> replaceDefinition(
    ArangoEdgeDefinition definition, {
    bool? waitForSync,
    bool? dropCollections,
  }) async {
    assert(definition.collection == this);

    final response = await _connection.request(
      method: 'PUT',
      path: '$_basePath#definition',
      queries: {
        if (waitForSync != null) 'waitForSync': waitForSync.toString(),
        if (dropCollections != null)
          'dropCollections': dropCollections.toString(),
      },
      body: definition.toJson(),
    );
    return response.body;
  }

  Future<Map<String, dynamic>> removeCollectionFromGraph({
    bool? waitForSync,
    bool? dropCollections,
  }) async {
    final response = await _connection.request(
      method: 'DELETE',
      path: '$_basePath#definition',
      queries: {
        if (waitForSync != null) 'waitForSync': waitForSync.toString(),
        if (dropCollections != null)
          'dropCollections': dropCollections.toString(),
      },
    );
    return response.body;
  }
}
