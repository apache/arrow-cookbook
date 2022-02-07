.. _arrow-flight:

============
Arrow Flight
============

Recipes related to leveraging Arrow Flight protocol

.. contents::

Simple Service with Arrow Flight
================================

We are going to create: Flight Producer and Flight Server:

* InMemoryStore: A FlightProducer that hosts an in memory store of Arrow buffers. Used for integration testing.

* ExampleFlightServer: An Example Flight Server that provides access to the InMemoryStore.

Creating the Server
*******************

.. testcode::

    // Creating the server
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.example.ExampleFlightServer;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;

    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    ExampleFlightServer efs = new ExampleFlightServer(allocator, Location.forGrpcInsecure("localhost", 33333));
    efs.start();

    System.out.println(efs.getLocation());

.. testoutput::

    Location{uri=grpc+tcp://localhost:33333}

Creating the Client
*******************

Get List Actions
----------------

Validate lists actions available on the Flight service:

.. testcode::

    // Creating the server
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.example.ExampleFlightServer;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;

    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    ExampleFlightServer efs = new ExampleFlightServer(allocator, Location.forGrpcInsecure("localhost", 33333));
    efs.start();

    // Client creation
    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.RootAllocator;

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    FlightClient client = FlightClient.builder(rootAllocator, Location.forGrpcInsecure("localhost", 33333)).build();

    // Lists actions available on the Flight service.
    import java.util.ArrayList;
    import org.apache.arrow.flight.ActionType;

    List<String> actionTypes = new ArrayList<>();
    for (ActionType at : client.listActions()) {
        actionTypes.add(at.getType());
    }

    System.out.println(actionTypes);

.. testoutput::

    [get, put, drop]

Get List Flights
----------------

.. testcode::


    // Creating the server
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.example.ExampleFlightServer;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;

    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    ExampleFlightServer efs = new ExampleFlightServer(allocator, Location.forGrpcInsecure("localhost", 33333));
    efs.start();

    // Client creation
    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.RootAllocator;

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    FlightClient client = FlightClient.builder(rootAllocator, Location.forGrpcInsecure("localhost", 33333)).build();

    // Lists flight information
    import org.apache.arrow.flight.FlightInfo;
    import org.apache.arrow.flight.Criteria;

    Iterable<FlightInfo> listFlights = client.listFlights(Criteria.ALL);
    listFlights.forEach(t -> System.out.println(t));
    System.out.println("Any list flight availale at this moment");

.. testoutput::

    Any list flight availale at this moment

Put Data
--------

.. testcode::

    // Creating the server
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.example.ExampleFlightServer;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;

    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    ExampleFlightServer efs = new ExampleFlightServer(allocator, Location.forGrpcInsecure("localhost", 33333));
    efs.start();

    // Client creation
    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.RootAllocator;

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    FlightClient client = FlightClient.builder(rootAllocator, Location.forGrpcInsecure("localhost", 33333)).build();

    // Populate vector schema root
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import static java.util.Arrays.asList;
    import org.apache.arrow.flight.FlightDescriptor;

    // Create a column data type
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);

    // Create a definition
    Schema schemaPerson = new Schema(asList(name, age));
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);

    // Getting field vectors
    VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
    nameVector.allocateNew(3);
    nameVector.set(0, "david".getBytes());
    nameVector.set(1, "gladis".getBytes());
    nameVector.set(2, "juan".getBytes());
    nameVector.setValueCount(3);
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(3);
    ageVector.set(0, 10);
    ageVector.set(1, 20);
    ageVector.set(2, 30);
    ageVector.setValueCount(3);

    vectorSchemaRoot.setRowCount(3);

    // Exchange data.
    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.AsyncPutListener;

    FlightClient.ClientStreamListener listener = client.startPut(FlightDescriptor.path("hello"), vectorSchemaRoot, new AsyncPutListener());
    listener.putNext();
    listener.completed();
    listener.getResult();

    // Lists flight information updated
    import org.apache.arrow.flight.FlightInfo;
    import org.apache.arrow.flight.Criteria;

    Iterable<FlightInfo> listFlights = client.listFlights(Criteria.ALL);

    listFlights.forEach(t -> System.out.println("FlightInfo{schema=" + t.getSchema() + ", descriptor=" + t.getDescriptor() + ", endpoints=" + t.getEndpoints().get(0).getLocations() + ", records=" + t.getRecords() + "}"))

.. testoutput::

    FlightInfo{schema=Schema<name: Utf8, age: Int(32, true)>, descriptor=hello, endpoints=[Location{uri=grpc+tcp://localhost:33333}], records=3}

Get Info per Path
-----------------

.. testcode::

    // Creating the server
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.example.ExampleFlightServer;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;

    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    ExampleFlightServer efs = new ExampleFlightServer(allocator, Location.forGrpcInsecure("localhost", 33333));
    efs.start();

    // Client creation
    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.RootAllocator;

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    FlightClient client = FlightClient.builder(rootAllocator, Location.forGrpcInsecure("localhost", 33333)).build();

    // Populate vector schema root
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import static java.util.Arrays.asList;

    // Create a column data type
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);

    // Create a definition
    Schema schemaPerson = new Schema(asList(name, age));
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);

    // Getting field vectors
    VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
    nameVector.allocateNew(3);
    nameVector.set(0, "david".getBytes());
    nameVector.set(1, "gladis".getBytes());
    nameVector.set(2, "juan".getBytes());
    nameVector.setValueCount(3);
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(3);
    ageVector.set(0, 10);
    ageVector.set(1, 20);
    ageVector.set(2, 30);
    ageVector.setValueCount(3);

    vectorSchemaRoot.setRowCount(3);

    // Exchange data.
    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.AsyncPutListener;
    import org.apache.arrow.flight.FlightDescriptor;

    FlightClient.ClientStreamListener listener = client.startPut(FlightDescriptor.path("hello"), vectorSchemaRoot, new AsyncPutListener());
    listener.putNext();
    listener.completed();
    listener.getResult();

    // Get info por new path just created
    import org.apache.arrow.flight.FlightInfo;

    FlightInfo info = client.getInfo(FlightDescriptor.path("hello"));

    System.out.println("FlightInfo{schema=" + info.getSchema() + ", descriptor=" + info.getDescriptor() + ", endpoints=" + info.getEndpoints().get(0).getLocations() + ", records=" + info.getRecords() + "}");

.. testoutput::

    FlightInfo{schema=Schema<name: Utf8, age: Int(32, true)>, descriptor=hello, endpoints=[Location{uri=grpc+tcp://localhost:33333}], records=3}

Request Data
------------

.. testcode::

    // Creating the server
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.example.ExampleFlightServer;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;

    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    ExampleFlightServer efs = new ExampleFlightServer(allocator, Location.forGrpcInsecure("localhost", 33333));
    efs.start();

    // Client creation
    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.RootAllocator;

    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    FlightClient client = FlightClient.builder(rootAllocator, Location.forGrpcInsecure("localhost", 33333)).build();

    // Populate vector schema root
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import static java.util.Arrays.asList;
    import org.apache.arrow.flight.FlightDescriptor;

    // Create a column data type
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);

    // Create a definition
    Schema schemaPerson = new Schema(asList(name, age));
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);

    // Getting field vectors
    VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
    nameVector.allocateNew(3);
    nameVector.set(0, "david".getBytes());
    nameVector.set(1, "gladis".getBytes());
    nameVector.set(2, "juan".getBytes());
    nameVector.setValueCount(3);
    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    ageVector.allocateNew(3);
    ageVector.set(0, 10);
    ageVector.set(1, 20);
    ageVector.set(2, 30);
    ageVector.setValueCount(3);

    vectorSchemaRoot.setRowCount(3);

    // Exchange data.
    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.AsyncPutListener;

    FlightClient.ClientStreamListener listener = client.startPut(FlightDescriptor.path("hello"), vectorSchemaRoot, new AsyncPutListener());
    listener.putNext();
    listener.completed();
    listener.getResult();

    // Get info por new path just created
    import org.apache.arrow.flight.FlightInfo;

    FlightInfo info = client.getInfo(FlightDescriptor.path("hello"));

    // Request data per path
    import org.apache.arrow.flight.FlightStream;

    String dataResponse = "";
    FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket());
    while (stream.next()) {
        dataResponse = stream.getRoot().contentToTSVString();
    }

    System.out.print(dataResponse);

.. testoutput::

    name    age
    david    10
    gladis    20
    juan    30