import 'package:arango/arango.dart';
import 'package:arango/src/arango_connection.dart';
import 'package:arango/src/graph/arango_collections.dart';

class ArangoGraph {
  final ArangoConnection _connection;
  final String name;

  ArangoGraph(this.name, this._connection)
      : assert(_connection.arangoMajor >= 3);

  Future<Map<String, dynamic>> create({
    bool? waitForSync,
    List<ArangoEdgeDefinition>? edgeDefinitions,
    List<ArangoDocumentCollection>? orphanCollections,
    bool? isSmart,
    bool? isDisjoint,
    String? smartGraphAttribute,
    int? numberOfShards,
    int? replicationFactor,
    int? writeConcern,
  }) async {
    final body = {
      'name': name,
      if (edgeDefinitions != null)
        'edgeDefinitions': edgeDefinitions.map((it) => it.toJson()).toList(),
      if (orphanCollections != null)
        'orphanCollections': orphanCollections.map((it) => it.name).toList(),
      if (isSmart != null) 'isSmart': isSmart,
      if (isDisjoint != null) 'isDisjoint': isSmart,
      'options': {
        if (smartGraphAttribute != null)
          'smartGraphAttribute': smartGraphAttribute,
        if (numberOfShards != null) 'numberOfShards': numberOfShards,
        if (replicationFactor != null) 'replicationFactor': replicationFactor,
        if (writeConcern != null) 'writeConcern': writeConcern,
      },
    };

    final response = await _connection.request(
      method: 'POST',
      path: '/_api/gharial',
      body: body,
      queries: {
        if (waitForSync != null) 'waitForSync': waitForSync.toString(),
      },
    );
    return response.body;
  }

  Future<Map<String, dynamic>> get() async {
    final resp = await _connection.request(path: '/_api/gharial/$name');
    return resp.body;
  }

  Future<bool> exists() async {
    try {
      await get();
    } on ArangoError catch (e) {
      const GRAPH_NOT_FOUND = 1924;
      if (e.errorNum == GRAPH_NOT_FOUND) {
        return false;
      } else {
        rethrow;
      }
    }
    return true;
  }

  Future<Map<String, dynamic>> drop({bool? dropCollections}) async {
    final response = await _connection.request(
      method: 'DELETE',
      path: '/_api/gharial/$name',
      queries: {
        if (dropCollections != null)
          'dropCollections': dropCollections.toString(),
      },
    );
    return response.body;
  }

  //#region Vertex collections
  ArangoGraphVertexCollection vertexCollection(String name) =>
      ArangoGraphVertexCollection(this, name, _connection);

  Future<List<ArangoGraphVertexCollection>> listVertexCollections() async {
    final response =
        await _connection.request(path: '/_api/gharial/$name/vertex');
    return (response.body['collections'] as List<dynamic>)
        .map((it) =>
            ArangoGraphVertexCollection(this, it as String, _connection))
        .toList();
  }

  Future<Map<String, dynamic>> addVertexCollection(
    ArangoGraphVertexCollection collection,
  ) async {
    final response = await _connection.request(
      method: 'POST',
      path: '/_api/gharial/$name/vertex',
      body: {'collection': collection.name},
    );
    return response.body;
  }

  Future<Map<String, dynamic>> removeVertexCollection(
    ArangoGraphVertexCollection collection, {
    bool? dropCollection,
  }) async {
    final response = await _connection.request(
      method: 'DELETE',
      path: '/_api/gharial/$name/vertex/${collection.name}',
      queries: {
        if (dropCollection != null) 'dropCollection': dropCollection.toString(),
      },
    );
    return response.body;
  }
  //#endregion

  //#region Edge collections
  ArangoGraphEdgeCollection edgeCollection(String name) =>
      ArangoGraphEdgeCollection(this, name, _connection);

  Future<List<ArangoGraphEdgeCollection>> listEdgeCollections() async {
    final response =
        await _connection.request(path: '/_api/gharial/$name/edge');
    return (response.body['collections'] as List<dynamic>)
        .map((it) => ArangoGraphEdgeCollection(this, it as String, _connection))
        .toList();
  }

  Future<Map<String, dynamic>> addEdgeDefinition(
    ArangoEdgeDefinition definition,
  ) async {
    final response = await _connection.request(
      method: 'POST',
      path: '/_api/gharial/$name/edge',
      body: definition.toJson(),
    );
    return response.body;
  }

  Future<Map<String, dynamic>> replaceEdgeDefinition(
    ArangoEdgeDefinition definition, {
    bool? waitForSync,
    bool? dropCollections,
  }) async {
    final response = await _connection.request(
      method: 'PUT',
      path: '/_api/gharial/$name/edge/${definition.collection.name}#definition',
      queries: {
        if (waitForSync != null) 'waitForSync': waitForSync.toString(),
        if (dropCollections != null)
          'dropCollections': dropCollections.toString(),
      },
      body: definition.toJson(),
    );
    return response.body;
  }

  Future<Map<String, dynamic>> removeEdgeDefinition(
    String edgeDefinitionName, {
    bool? waitForSync,
    bool? dropCollections,
  }) async {
    final response = await _connection.request(
      method: 'DELETE',
      path: '/_api/gharial/$name/edge/$edgeDefinitionName#definition',
      queries: {
        if (waitForSync != null) 'waitForSync': waitForSync.toString(),
        if (dropCollections != null)
          'dropCollections': dropCollections.toString(),
      },
    );
    return response.body;
  }
  //#endregion
}

class ArangoEdgeDefinition {
  ArangoEdgeDefinition({
    required this.collection,
    required this.fromCollections,
    required this.toCollections,
  });

  final ArangoGraphVertexCollection collection;
  final List<ArangoDocumentCollection> fromCollections;
  final List<ArangoDocumentCollection> toCollections;

  Map<String, dynamic> toJson() {
    return {
      'collection': collection.name,
      'from': fromCollections.map((it) => it.name).toList(),
      'to': toCollections.map((it) => it.name).toList(),
    };
  }
}
