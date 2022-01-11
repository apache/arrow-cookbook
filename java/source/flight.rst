.. _arrow-flight:

============
Arrow Flight
============

Recipes related to leveraging Arrow Flight protocol

.. contents::

Simple Service with Arrow Flight
================================

Flight Producer and Server
**************************

We are going to create: Flight Producer and Fligh Server:

* InMemoryStore: A FlightProducer that hosts an in memory store of Arrow buffers. Used for integration testing.

* ExampleFlightServer: An Example Flight Server that provides access to the InMemoryStore.

InMemoryStore
-------------
A FlightProducer that hosts an in memory store of Arrow buffers. Used for integration testing.

.. code-block:: java

    jshell> /edit
    |  created class InMemoryStore

.. code-block:: java
   :name: UtilStore
   :emphasize-lines: 28,55-56,76-77,89-90,100-101,133-134,150-151,160-161

    import org.apache.arrow.flight.Action;
    import org.apache.arrow.flight.ActionType;
    import org.apache.arrow.flight.CallStatus;
    import org.apache.arrow.flight.Criteria;
    import org.apache.arrow.flight.FlightDescriptor;
    import org.apache.arrow.flight.FlightInfo;
    import org.apache.arrow.flight.FlightProducer;
    import org.apache.arrow.flight.FlightStream;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.PutResult;
    import org.apache.arrow.flight.Result;
    import org.apache.arrow.flight.Ticket;
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
                              FlightProducer.ServerStreamListener listener) {
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
   :emphasize-lines: 12,27

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

Creating the Server
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

Creating the Client
*******************

.. code-block:: java
   :name: Client
   :emphasize-lines: 5

    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.Location;

    // client creation
    FlightClient client = FlightClient.builder(allocator, Location.forGrpcInsecure("localhost", 33333)).build();


Transfer Data
*************

Get List Actions
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
   :emphasize-lines: 1

    jshell> actionTypes

    actionTypes ==> [get, put, drop]

Get List Flights
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


Put Data
--------

Lets create one VectorSchemaRoot using createVectorSchemaRoot method defined below:

.. code-block:: java
   :emphasize-lines: 21

   import org.apache.arrow.memory.RootAllocator;
   import org.apache.arrow.vector.BitVectorHelper;
   import org.apache.arrow.vector.IntVector;
   import org.apache.arrow.vector.VarCharVector;
   import org.apache.arrow.vector.VectorSchemaRoot;
   import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
   import org.apache.arrow.vector.complex.ListVector;
   import org.apache.arrow.vector.types.Types;
   import org.apache.arrow.vector.types.pojo.ArrowType;
   import org.apache.arrow.vector.types.pojo.Field;
   import org.apache.arrow.vector.types.pojo.FieldType;
   import org.apache.arrow.vector.types.pojo.Schema;

   import java.util.ArrayList;
   import java.util.HashMap;
   import java.util.List;
   import java.util.Map;

   import static java.util.Arrays.asList;

      void setVector(IntVector vector, Integer... values) {
       final int length = values.length;
       vector.allocateNew(length);
       for (int i = 0; i < length; i++) {
           if (values[i] != null) {
               vector.set(i, values[i]);
           }
       }
       vector.setValueCount(length);
   }

   void setVector(VarCharVector vector, byte[]... values) {
       final int length = values.length;
       vector.allocateNewSafe();
       for (int i = 0; i < length; i++) {
           if (values[i] != null) {
               vector.set(i, values[i]);
           }
       }
       vector.setValueCount(length);
   }

   void setVector(ListVector vector, List<Integer>... values) {
       vector.allocateNewSafe();
       Types.MinorType type = Types.MinorType.INT;
       vector.addOrGetVector(FieldType.nullable(type.getType()));

       IntVector dataVector = (IntVector) vector.getDataVector();
       dataVector.allocateNew();

       // set underlying vectors
       int curPos = 0;
       vector.getOffsetBuffer().setInt(0, curPos);
       for (int i = 0; i < values.length; i++) {
           if (values[i] == null) {
               BitVectorHelper.unsetBit(vector.getValidityBuffer(), i);
           } else {
               BitVectorHelper.setBit(vector.getValidityBuffer(), i);
               for (int value : values[i]) {
                   dataVector.setSafe(curPos, value);
                   curPos += 1;
               }
           }
           vector.getOffsetBuffer().setInt((i + 1) * BaseRepeatedValueVector.OFFSET_WIDTH, curPos);
       }
       dataVector.setValueCount(curPos);
       vector.setLastSet(values.length - 1);
       vector.setValueCount(values.length);
   }

   VectorSchemaRoot createVectorSchemaRoot(){
       // create a column data type
       Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);

       Map<String, String> metadata = new HashMap<>();
       metadata.put("A", "Id card");
       metadata.put("B", "Passport");
       metadata.put("C", "Visa");
       Field document = new Field("document", new FieldType(true, new ArrowType.Utf8(), null, metadata), null);

       Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);

       FieldType intType = new FieldType(true, new ArrowType.Int(32, true), /*dictionary=*/null);
       FieldType listType = new FieldType(true, new ArrowType.List(), /*dictionary=*/null);
       Field childField = new Field("intCol", intType, null);
       List<Field> childFields = new ArrayList<>();
       childFields.add(childField);
       Field points = new Field("points", listType, childFields);

       // create a definition
       Schema schemaPerson = new Schema(asList(name, document, age, points));

       RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE); // deal with byte buffer allocation
       VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);

       // getting field vectors
       VarCharVector nameVectorOption1 = (VarCharVector) vectorSchemaRoot.getVector("name"); //interface FieldVector
       VarCharVector documentVectorOption1 = (VarCharVector) vectorSchemaRoot.getVector("document"); //interface FieldVector
       IntVector ageVectorOption1 = (IntVector) vectorSchemaRoot.getVector("age");
       ListVector pointsVectorOption1 = (ListVector) vectorSchemaRoot.getVector("points");

       // add values to the field vectors
       setVector(nameVectorOption1, "david".getBytes(), "gladis".getBytes(), "juan".getBytes());
       setVector(documentVectorOption1, "A".getBytes(), "B".getBytes(), "C".getBytes());
       setVector(ageVectorOption1, 10,20,30);
       setVector(pointsVectorOption1, asList(1,3,5,7,9), asList(2,4,6,8,10), asList(1,2,3,5,8));
       vectorSchemaRoot.setRowCount(3);

       return vectorSchemaRoot;
   }

.. code-block:: 
   :emphasize-lines: 1,5

    jshell> VectorSchemaRoot vectorSchemaRoot = createVectorSchemaRoot();

    vectorSchemaRoot ==> org.apache.arrow.vector.VectorSchemaRoot@3d1848cc

    jshell> System.out.println(vectorSchemaRoot.contentToTSVString())

    name     document age   points
    david    A        10    [1,3,5,7,9]
    gladis   B        20    [2,4,6,8,10]
    juan     C        30    [1,2,3,5,8]

Let transfer data of vectorSchemaRoot:

.. code-block:: java
   :emphasize-lines: 12,20,25,31

    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.AsyncPutListener;

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
   :emphasize-lines: 1

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

Get Info per Path
-----------------

.. code-block:: java
   :emphasize-lines: 7

    import org.apache.arrow.flight.FlightInfo;

    /**
     * 3.- Get info por new path just created
     */

    FlightInfo info = client.getInfo(FlightDescriptor.path("hello"));

.. code-block:: java
   :emphasize-lines: 1

   jshell> info

   info ==> FlightInfo{schema=Schema<name: Utf8, document: Utf8, age: Int(32, true), points: List<intCol: Int(32, true)>>, descriptor=hello, endpoints=[FlightEndpoint{locations=[Location{uri=grpc+tcp://localhost:33333}], ticket=org.apache.arrow.flight.Ticket@7af6ad9c}], bytes=266, records=3}

Request Data
------------

.. code-block:: java
   :emphasize-lines: 9

    import org.apache.arrow.flight.FlightStream;

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
   :emphasize-lines: 1

    jshell> System.out.println(dataResponse);

    name    document    age points
    david   A   10  [1,3,5,7,9]
    gladis  B   20  [2,4,6,8,10]
    juan    C   30  [1,2,3,5,8]


