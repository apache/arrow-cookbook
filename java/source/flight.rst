.. _arrow-flight:

============
Arrow Flight
============

Recipes related to leveraging Arrow Flight protocol

.. contents::

Simple service with Arrow Flight
================================

Common classes
**************

| We are going to use this util for data manipulation:
|
| - InMemoryStore: A FlightProducer that hosts an in memory store of Arrow buffers. Used for integration testing.
|
| - ExampleFlightServer: An Example Flight Server that provides access to the InMemoryStore.

InMemoryStore
-------------
A FlightProducer that hosts an in memory store of Arrow buffers. Used for integration testing.

.. code-block:: java

    jshell> /edit
    |  created class InMemoryStore

.. code-block:: java
   :name: UtilStore
   :emphasize-lines: 49-50,56-59,70-71,83-84,94-96,127-129,144-146,154-155


    //*************************************************************************************************
    // A FlightProducer that hosts an in memory store of Arrow buffers. Used for integration testing. *
    //*************************************************************************************************


    import org.apache.arrow.flight.*;
    import org.apache.arrow.flight.example.ExampleTicket;
    import org.apache.arrow.flight.example.FlightHolder;
    import org.apache.arrow.flight.example.Stream;
    import org.apache.arrow.flight.example.Stream.StreamCreator;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.util.AutoCloseables;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.VectorUnloader;

    import java.util.concurrent.ConcurrentHashMap;
    import java.util.concurrent.ConcurrentMap;

    /**
     * A FlightProducer that hosts an in memory store of Arrow buffers. Used for integration testing.
     */
    public class InMemoryStore implements FlightProducer, AutoCloseable {

      private final ConcurrentMap<FlightDescriptor, FlightHolder> holders = new ConcurrentHashMap<>();
      private final BufferAllocator allocator;
      private Location location;

      /**
       * Constructs a new instance.
       *
       * @param allocator The allocator for creating new Arrow buffers.
       * @param location The location of the storage.
       */
      public InMemoryStore(BufferAllocator allocator, Location location) {
        super();
        this.allocator = allocator;
        this.location = location;
      }

      /**
       * Update the location after server start.
       *
       * <p>Useful for binding to port 0 to get a free port.
       */
      public void setLocation(Location location) {
        this.location = location;
      }

      @Override
      public void getStream(CallContext context, Ticket ticket,
          ServerStreamListener listener) {
        System.out.println("Calling to getStream");
        getStream(ticket).sendTo(allocator, listener);
      }

      /**
       * Returns the appropriate stream given the ticket (streams are indexed by path and an ordinal).
       */
      public Stream getStream(Ticket t) {
        ExampleTicket example = ExampleTicket.from(t);
        FlightDescriptor d = FlightDescriptor.path(example.getPath());
        FlightHolder h = holders.get(d);
        if (h == null) {
          throw new IllegalStateException("Unknown ticket.");
        }

        return h.getStream(example);
      }

      @Override
      public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        System.out.println("Calling to listFligths");
        try {
          for (FlightHolder h : holders.values()) {
            listener.onNext(h.getFlightInfo(location));
          }
          listener.onCompleted();
        } catch (Exception ex) {
          listener.onError(ex);
        }
      }

      @Override
      public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        System.out.println("Calling to getFlightInfo");
        FlightHolder h = holders.get(descriptor);
        if (h == null) {
          throw new IllegalStateException("Unknown descriptor.");
        }

        return h.getFlightInfo(location);
      }

      @Override
      public Runnable acceptPut(CallContext context,
          final FlightStream flightStream, final StreamListener<PutResult> ackStream) {
        return () -> {
          System.out.println("Calling to acceptPut");
          StreamCreator creator = null;
          boolean success = false;
          try (VectorSchemaRoot root = flightStream.getRoot()) {
            final FlightHolder h = holders.computeIfAbsent(
                flightStream.getDescriptor(),
                t -> new FlightHolder(allocator, t, flightStream.getSchema(), flightStream.getDictionaryProvider()));

            creator = h.addStream(flightStream.getSchema());

            VectorUnloader unloader = new VectorUnloader(root);
            while (flightStream.next()) {
              ackStream.onNext(PutResult.metadata(flightStream.getLatestMetadata()));
              creator.add(unloader.getRecordBatch());
            }
            // Closing the stream will release the dictionaries
            flightStream.takeDictionaryOwnership();
            creator.complete();
            success = true;
          } finally {
            if (!success) {
              creator.drop();
            }
          }

        };

      }

      @Override
      public void doAction(CallContext context, Action action,
          StreamListener<Result> listener) {
        System.out.println("Calling to doAction");
        switch (action.getType()) {
          case "drop": {
            // not implemented.
            listener.onNext(new Result(new byte[0]));
            listener.onCompleted();
            break;
          }
          default: {
            listener.onError(CallStatus.UNIMPLEMENTED.toRuntimeException());
          }
        }
      }

      @Override
      public void listActions(CallContext context,
          StreamListener<ActionType> listener) {
        System.out.println("Calling to listActions");
        listener.onNext(new ActionType("get", "pull a stream. Action must be done via standard get mechanism"));
        listener.onNext(new ActionType("put", "push a stream. Action must be done via standard put mechanism"));
        listener.onNext(new ActionType("drop", "delete a flight. Action body is a JSON encoded path."));
        listener.onCompleted();
      }

      @Override
      public void close() throws Exception {
        System.out.println("Calling to close");
        AutoCloseables.close(holders.values());
        holders.clear();
      }

    }

ExampleFlightServer
-------------------
An Example Flight Server that provides access to the InMemoryStore.

.. code-block:: java

    jshell> /edit
    |  created class ExampleFlightServer

.. code-block:: java
   :name: UtilServer
   :emphasize-lines: 33

    //****************************************************************************************************
    // An Example Flight Server that provides access to the InMemoryStore. Used for integration testing. *
    //****************************************************************************************************

    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;

    import java.io.IOException;

    /**
     * An Example Flight Server that provides access to the InMemoryStore. Used for integration testing.
     */
    public class ExampleFlightServer implements AutoCloseable {

      private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExampleFlightServer.class);

      private final FlightServer flightServer;
      private final Location location;
      private final BufferAllocator allocator;
      private final InMemoryStore mem;

      /**
       * Constructs a new instance using Allocator for allocating buffer storage that binds
       * to the given location.
       */
      public ExampleFlightServer(BufferAllocator allocator, Location location) {
        this.allocator = allocator.newChildAllocator("flight-server", 0, Long.MAX_VALUE);
        this.location = location;
        this.mem = new InMemoryStore(this.allocator, location);
        this.flightServer = FlightServer.builder(allocator, location, mem).build();
      }

      public Location getLocation() {
        return location;
      }

      public int getPort() {
        return this.flightServer.getPort();
      }

      public void start() throws IOException {
        flightServer.start();
      }

      public void awaitTermination() throws InterruptedException {
        flightServer.awaitTermination();
      }

      public InMemoryStore getStore() {
        return mem;
      }

      @Override
      public void close() throws Exception {
        AutoCloseables.close(mem, flightServer, allocator);
      }
    }

Creating the server
*******************

.. code-block:: java
   :name: Server
   :emphasize-lines: 9

    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;

    // server creation
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    ExampleFlightServer efs = new ExampleFlightServer(allocator, Location.forGrpcInsecure("localhost", 33333));
    efs.start();

Creating the client
*******************

.. code-block:: java
   :name: Client
   :emphasize-lines: 5

    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.Location;

    // client creation
    FlightClient client = FlightClient.builder(allocator, Location.forGrpcInsecure("localhost", 33333)).build();


Transfer data
*************

Get list actions
----------------

Validate lists actions available on the Flight service:

.. code-block:: java
   :emphasize-lines: 7

    import java.util.ArrayList;

    /**
     * 0.- Lists actions available on the Flight service.
     */
    List<String> actionTypes = new ArrayList<>();
    for (ActionType at : client.listActions()) {
        actionTypes.add(at.getType());
    }

.. code-block:: java
   :emphasize-lines: 1-3

    jshell> actionTypes

    actionTypes ==> [get, put, drop]

Get list flights
----------------

.. code-block:: java
   :emphasize-lines: 4

    /**
     * 1.- Lists flight information.
     */
    Iterable<FlightInfo> listFlights = client.listFlights(Criteria.ALL);

.. code-block:: java
   :emphasize-lines: 1

    jshell> listFlights.forEach(t -> System.out.println(t));


Put data
--------

Consider: Populate VectorSchemaRoot as it was created at :ref:`arrow-io` to create vectorSchemaRoot variable: VectorSchemaRoot vectorSchemaRoot = createVectorSchemaRoot();

.. code-block:: java
   :emphasize-lines: 1

   VectorSchemaRoot vectorSchemaRoot = createVectorSchemaRoot();

.. code-block:: java
   :emphasize-lines: 1-6

   jshell> System.out.println(vectorSchemaRoot.contentToTSVString())

   name     document age   points
   david    A        10    [1,3,5,7,9]
   gladis   B        20    [2,4,6,8,10]
   juan     C        30    [1,2,3,5,8]

Let transfer data of vectorSchemaRoot:

.. code-block:: java
   :emphasize-lines: 12,20,25,31

    import org.apache.arrow.flight.FlightClient;

    /**
     * 2.- Exchange data.
     */

    /**
     * An identifier for a particular set of data.  This can either be an opaque command that generates
     * the data or a static "path" to the data.  This is a POJO wrapper around the protobuf message with
     * the same name.
     */
    FlightClient.ClientStreamListener listener = client.startPut(FlightDescriptor.path("hello"), vectorSchemaRoot, new AsyncPutListener());

    /**
     * Send the current contents of the associated {@link VectorSchemaRoot}.
     *
     * <p>This will not necessarily block until the message is actually sent; it may buffer messages
     * in memory. Use {@link #isReady()} to check if there is backpressure and avoid excessive buffering.
     */
    listener.putNext();

    /**
     * Indicate that transmission is finished.
     */
    listener.completed();

    /**
     * Wait for the stream to finish on the server side. You must call this to be notified of any errors that may have
     * happened during the upload.
     */
    listener.getResult();

Get list actions updated:

.. code-block:: java
   :emphasize-lines: 4

    /**
     * 3.- Lists flight information updated.
     */
    listFlights = client.listFlights(Criteria.ALL);

.. code-block:: java
   :emphasize-lines: 1-13

    jshell> listFlights.forEach(t -> System.out.println(t));

    FlightInfo{
        schema=Schema<name: Utf8, document: Utf8, age: Int(32, true), points: List<intCol: Int(32, true)>>, descriptor=hello, 
        endpoints=[
            FlightEndpoint{
                locations=[Location{uri=grpc+tcp://localhost:33333}], 
                ticket=org.apache.arrow.flight.Ticket@c39eb3c2
            }
        ], 
        bytes=266, 
        records=3
    }

Get info per path
-----------------

.. code-block:: java
   :emphasize-lines: 7

    import org.apache.arrow.flight.*;

    /**
     * 3.- Get info por new path just created
     */

    FlightInfo info = client.getInfo(FlightDescriptor.path("hello"));

.. code-block:: java
   :emphasize-lines: 1-3

   jshell> info

   info ==> FlightInfo{schema=Schema<name: Utf8, document: Utf8, age: Int(32, true), points: List<intCol: Int(32, true)>>, descriptor=hello, endpoints=[FlightEndpoint{locations=[Location{uri=grpc+tcp://localhost:33333}], ticket=org.apache.arrow.flight.Ticket@7af6ad9c}], bytes=266, records=3}

Request data
------------

.. code-block:: java
   :emphasize-lines: 9

    import org.apache.arrow.flight.*;

    /**
     * 4.- Request data per path
     */

    String dataResponse;

    FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket());
    // do whatever with VectorSchemaRoot response: stream.getRoot()
    while (stream.next()) {
        dataResponse = stream.getRoot().contentToTSVString();
    }

.. code-block:: java
   :emphasize-lines: 1-6

    jshell> System.out.println(dataResponse);

    name    document    age points
    david   A   10  [1,3,5,7,9]
    gladis  B   20  [2,4,6,8,10]
    juan    C   30  [1,2,3,5,8]


