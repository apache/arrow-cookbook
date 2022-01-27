.. _arrow-flight:

============
Arrow Flight
============

Recipes related to leveraging Arrow Flight protocol

.. contents::

Simple Service with Arrow Flight
================================

We are going to create: Flight Producer and Fligh Server:

* InMemoryStore: A FlightProducer that hosts an in memory store of Arrow buffers. Used for integration testing.

* ExampleFlightServer: An Example Flight Server that provides access to the InMemoryStore.

Creating the Server
*******************

.. testcode::

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

    // InMemoryStore

    public class InMemoryStore implements FlightProducer, AutoCloseable {
        private final ConcurrentMap<FlightDescriptor, FlightHolder> holders = new ConcurrentHashMap<>();
        private final BufferAllocator allocator;
        private Location location;

        public InMemoryStore(BufferAllocator allocator, Location location) {
            super();
            this.allocator = allocator;
            this.location = location;
        }

        public void setLocation(Location location) {
            this.location = location;
        }

        @Override
        public void getStream(CallContext context, Ticket ticket,
                              FlightProducer.ServerStreamListener listener) {
            System.out.println("Calling to getStream");
            getStream(ticket).sendTo(allocator, listener);
        }

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


    // ExampleFlightServer

    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;

    import java.io.IOException;

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

    // Creating the Server

    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;

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

    // create the server
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

    // InMemoryStore

    public class InMemoryStore implements FlightProducer, AutoCloseable {
        private final ConcurrentMap<FlightDescriptor, FlightHolder> holders = new ConcurrentHashMap<>();
        private final BufferAllocator allocator;
        private Location location;

        public InMemoryStore(BufferAllocator allocator, Location location) {
            super();
            this.allocator = allocator;
            this.location = location;
        }

        public void setLocation(Location location) {
            this.location = location;
        }

        @Override
        public void getStream(CallContext context, Ticket ticket,
                              FlightProducer.ServerStreamListener listener) {
            System.out.println("Calling to getStream");
            getStream(ticket).sendTo(allocator, listener);
        }

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


    // ExampleFlightServer

    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;

    import java.io.IOException;

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

    // Creating the Server

    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;

    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    ExampleFlightServer efs = new ExampleFlightServer(allocator, Location.forGrpcInsecure("localhost", 33333));
    efs.start();

    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.flight.ActionType;

    // client creation
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    FlightClient client = FlightClient.builder(rootAllocator, Location.forGrpcInsecure("localhost", 33333)).build();

    /**
     * Lists actions available on the Flight service.
     */
    import java.util.ArrayList;
    List<String> actionTypes = new ArrayList<>();
    for (ActionType at : client.listActions()) {
        actionTypes.add(at.getType());
    }

    System.out.println(actionTypes);

.. testoutput::

    Calling to listActions
    [get, put, drop]

Get List Flights
----------------

.. testcode::

    // create the server
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

    // InMemoryStore

    public class InMemoryStore implements FlightProducer, AutoCloseable {
        private final ConcurrentMap<FlightDescriptor, FlightHolder> holders = new ConcurrentHashMap<>();
        private final BufferAllocator allocator;
        private Location location;

        public InMemoryStore(BufferAllocator allocator, Location location) {
            super();
            this.allocator = allocator;
            this.location = location;
        }

        public void setLocation(Location location) {
            this.location = location;
        }

        @Override
        public void getStream(CallContext context, Ticket ticket,
                              FlightProducer.ServerStreamListener listener) {
            System.out.println("Calling to getStream");
            getStream(ticket).sendTo(allocator, listener);
        }

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


    // ExampleFlightServer

    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;

    import java.io.IOException;

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

    // Creating the Server

    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;

    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    ExampleFlightServer efs = new ExampleFlightServer(allocator, Location.forGrpcInsecure("localhost", 33333));
    efs.start();

    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.RootAllocator;

    // client creation
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    FlightClient client = FlightClient.builder(rootAllocator, Location.forGrpcInsecure("localhost", 33333)).build();

    /**
     * Lists flight information.
     */
    Iterable<FlightInfo> listFlights = client.listFlights(Criteria.ALL);

    listFlights.forEach(t -> System.out.println(t));
    System.out.println("Any list flight availale at this moment");

.. testoutput::

    Calling to listFligths
    Any list flight availale at this moment

Put Data
--------

.. testcode::

    // create the server
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

    // InMemoryStore

    public class InMemoryStore implements FlightProducer, AutoCloseable {
        private final ConcurrentMap<FlightDescriptor, FlightHolder> holders = new ConcurrentHashMap<>();
        private final BufferAllocator allocator;
        private Location location;

        public InMemoryStore(BufferAllocator allocator, Location location) {
            super();
            this.allocator = allocator;
            this.location = location;
        }

        public void setLocation(Location location) {
            this.location = location;
        }

        @Override
        public void getStream(CallContext context, Ticket ticket,
                              FlightProducer.ServerStreamListener listener) {
            System.out.println("Calling to getStream");
            getStream(ticket).sendTo(allocator, listener);
        }

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


    // ExampleFlightServer

    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;

    import java.io.IOException;

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

    // Creating the Server

    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;

    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    ExampleFlightServer efs = new ExampleFlightServer(allocator, Location.forGrpcInsecure("localhost", 33333));
    efs.start();

    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.RootAllocator;

    // client creation
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    FlightClient client = FlightClient.builder(rootAllocator, Location.forGrpcInsecure("localhost", 33333)).build();

    /**
     * Populate vector schema root
     */
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import static java.util.Arrays.asList;

    // create a column data type
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);

    // create a definition
    Schema schemaPerson = new Schema(asList(name, age));
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);

    // getting field vectors
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

    /**
     * Exchange data.
     */
    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.AsyncPutListener;

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

    /**
     * Lists flight information updated.
     */
    Iterable<FlightInfo> listFlights = client.listFlights(Criteria.ALL);

    listFlights.forEach(t -> System.out.println("FlightInfo{schema=" + t.getSchema() + ", descriptor=" + t.getDescriptor() + ", endpoints=" + t.getEndpoints().get(0).getLocations() + ", records=" + t.getRecords() + "}"))

.. testoutput::

    Calling to acceptPut
    Calling to listFligths
    FlightInfo{schema=Schema<name: Utf8, age: Int(32, true)>, descriptor=hello, endpoints=[Location{uri=grpc+tcp://localhost:33333}], records=3}

Get Info per Path
-----------------

.. testcode::

    // create the server
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

    // InMemoryStore

    public class InMemoryStore implements FlightProducer, AutoCloseable {
        private final ConcurrentMap<FlightDescriptor, FlightHolder> holders = new ConcurrentHashMap<>();
        private final BufferAllocator allocator;
        private Location location;

        public InMemoryStore(BufferAllocator allocator, Location location) {
            super();
            this.allocator = allocator;
            this.location = location;
        }

        public void setLocation(Location location) {
            this.location = location;
        }

        @Override
        public void getStream(CallContext context, Ticket ticket,
                              FlightProducer.ServerStreamListener listener) {
            System.out.println("Calling to getStream");
            getStream(ticket).sendTo(allocator, listener);
        }

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


    // ExampleFlightServer

    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;

    import java.io.IOException;

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

    // Creating the Server

    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;

    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    ExampleFlightServer efs = new ExampleFlightServer(allocator, Location.forGrpcInsecure("localhost", 33333));
    efs.start();

    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.FlightDescriptor;
    import org.apache.arrow.memory.RootAllocator;

    // client creation
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    FlightClient client = FlightClient.builder(rootAllocator, Location.forGrpcInsecure("localhost", 33333)).build();

    /**
     * Populate vector schema root
     */
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import static java.util.Arrays.asList;

    // create a column data type
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);

    // create a definition
    Schema schemaPerson = new Schema(asList(name, age));
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);

    // getting field vectors
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

    /**
     * Exchange data.
     */
    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.AsyncPutListener;

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

    /**
     * Get info por new path just created
     */
    import org.apache.arrow.flight.FlightInfo;

    FlightInfo info = client.getInfo(FlightDescriptor.path("hello"));

    System.out.println("FlightInfo{schema=" + info.getSchema() + ", descriptor=" + info.getDescriptor() + ", endpoints=" + info.getEndpoints().get(0).getLocations() + ", records=" + info.getRecords() + "}");

.. testoutput::

    Calling to acceptPut
    Calling to getFlightInfo
    FlightInfo{schema=Schema<name: Utf8, age: Int(32, true)>, descriptor=hello, endpoints=[Location{uri=grpc+tcp://localhost:33333}], records=3}

Request Data
------------

.. testcode::

    // create the server
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

    // InMemoryStore

    public class InMemoryStore implements FlightProducer, AutoCloseable {
        private final ConcurrentMap<FlightDescriptor, FlightHolder> holders = new ConcurrentHashMap<>();
        private final BufferAllocator allocator;
        private Location location;

        public InMemoryStore(BufferAllocator allocator, Location location) {
            super();
            this.allocator = allocator;
            this.location = location;
        }

        public void setLocation(Location location) {
            this.location = location;
        }

        @Override
        public void getStream(CallContext context, Ticket ticket,
                              FlightProducer.ServerStreamListener listener) {
            System.out.println("Calling to getStream");
            getStream(ticket).sendTo(allocator, listener);
        }

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


    // ExampleFlightServer

    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;

    import java.io.IOException;

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

    // Creating the Server

    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;

    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    ExampleFlightServer efs = new ExampleFlightServer(allocator, Location.forGrpcInsecure("localhost", 33333));
    efs.start();

    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.FlightDescriptor;
    import org.apache.arrow.memory.RootAllocator;

    // client creation
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    FlightClient client = FlightClient.builder(rootAllocator, Location.forGrpcInsecure("localhost", 33333)).build();

    /**
     * Populate vector schema root
     */
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import static java.util.Arrays.asList;

    // create a column data type
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);

    // create a definition
    Schema schemaPerson = new Schema(asList(name, age));
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator);

    // getting field vectors
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

    /**
     * Exchange data.
     */
    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.AsyncPutListener;

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

    /**
     * Get info por new path just created
     */
    import org.apache.arrow.flight.FlightInfo;

    FlightInfo info = client.getInfo(FlightDescriptor.path("hello"));

    /**
     * Request data per path
     */
    import org.apache.arrow.flight.FlightStream;
    String dataResponse = "";
    FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket());
    // do whatever with VectorSchemaRoot response: stream.getRoot()
    while (stream.next()) {
        dataResponse = stream.getRoot().contentToTSVString();
    }

    System.out.println(dataResponse);

.. testoutput::

    Calling to acceptPut
    Calling to getFlightInfo
    Calling to getStream
    name    age
    david    10
    gladis    20
    juan    30