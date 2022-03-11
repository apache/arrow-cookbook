.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

============
Arrow Flight
============

This section contains a number of recipes for working with Arrow Flight.
For more detail about Flight please take a look at `Arrow Flight RPC`_.

.. contents::

Simple Key-Value Storage Service with Arrow Flight
==================================================

We'll implement a service that provides a key-value store for data, using Flight to handle uploads/requests
and data in memory to store the actual data.

Flight Client and Server
************************

.. testcode::

    import org.apache.arrow.flight.Action;
    import org.apache.arrow.flight.AsyncPutListener;
    import org.apache.arrow.flight.CallStatus;
    import org.apache.arrow.flight.Criteria;
    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.FlightDescriptor;
    import org.apache.arrow.flight.FlightEndpoint;
    import org.apache.arrow.flight.FlightInfo;
    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.FlightStream;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.NoOpFlightProducer;
    import org.apache.arrow.flight.PutResult;
    import org.apache.arrow.flight.Result;
    import org.apache.arrow.flight.Ticket;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.VectorLoader;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.VectorUnloader;
    import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.Schema;

    import java.io.IOException;
    import java.nio.charset.StandardCharsets;
    import java.util.ArrayList;
    import java.util.Arrays;
    import java.util.Collections;
    import java.util.Iterator;
    import java.util.List;
    import java.util.concurrent.ConcurrentHashMap;

    class Dataset {
        private final List<ArrowRecordBatch> batches;
        private final Schema schema;
        private final long rows;
        public Dataset(List<ArrowRecordBatch> batches, Schema schema, long rows) {
            this.batches = batches;
            this.schema = schema;
            this.rows = rows;
        }
        public List<ArrowRecordBatch> getBatches() {
            return batches;
        }
        public Schema getSchema() {
            return schema;
        }
        public long getRows() {
            return rows;
        }
    }
    class CookbookProducer extends NoOpFlightProducer {
        private final RootAllocator allocator;
        private final Location location;
        private final ConcurrentHashMap<FlightDescriptor, Dataset> datasets;
        public CookbookProducer(RootAllocator allocator, Location location) {
            this.allocator = allocator;
            this.location = location;
            this.datasets = new ConcurrentHashMap<>();
        }
        @Override
        public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
            List<ArrowRecordBatch> batches = new ArrayList<>();
            return () -> {
                long rows = 0;
                VectorUnloader unloader;
                while (flightStream.next()) {
                    unloader = new VectorUnloader(flightStream.getRoot());
                    try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
                        batches.add(arb);
                        rows += flightStream.getRoot().getRowCount();
                    }
                }
                Dataset dataset = new Dataset(batches, flightStream.getSchema(), rows);
                datasets.put(flightStream.getDescriptor(), dataset);
                ackStream.onCompleted();
            };
        }

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
            FlightDescriptor flightDescriptor = FlightDescriptor.path(
                    new String(ticket.getBytes(), StandardCharsets.UTF_8));
            Dataset dataset = this.datasets.get(flightDescriptor);
            if (dataset == null) {
                throw CallStatus.NOT_FOUND.withDescription("Unknown descriptor").toRuntimeException();
            } else {
                VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(
                        this.datasets.get(flightDescriptor).getSchema(), allocator);
                listener.start(vectorSchemaRoot);
                for (ArrowRecordBatch arrowRecordBatch : this.datasets.get(flightDescriptor).getBatches()) {
                    VectorLoader loader = new VectorLoader(vectorSchemaRoot);
                    loader.load(arrowRecordBatch.cloneWithTransfer(allocator));
                    listener.putNext();
                }
                listener.completed();
            }
        }

        @Override
        public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
            FlightDescriptor flightDescriptor = FlightDescriptor.path(
                    new String(action.getBody(), StandardCharsets.UTF_8));
            switch (action.getType()) {
                case "DELETE":
                    if (datasets.remove(flightDescriptor) != null) {
                        Result result = new Result("Delete completed".getBytes(StandardCharsets.UTF_8));
                        listener.onNext(result);
                    } else {
                        Result result = new Result("Delete not completed. Reason: Key did not exist."
                                .getBytes(StandardCharsets.UTF_8));
                        listener.onNext(result);
                    }
                    listener.onCompleted();
            }
        }

        @Override
        public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
            FlightEndpoint flightEndpoint = new FlightEndpoint(
                    new Ticket(descriptor.getPath().get(0).getBytes(StandardCharsets.UTF_8)), location);
            return new FlightInfo(
                    datasets.get(descriptor).getSchema(),
                    descriptor,
                    Collections.singletonList(flightEndpoint),
                    /*bytes=*/-1,
                    datasets.get(descriptor).getRows()
            );
        }

        @Override
        public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
            datasets.forEach((k, v) -> { listener.onNext(getFlightInfo(null, k)); });
            listener.onCompleted();
        }
    }
    Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)){
        // Server
        try(FlightServer flightServer = FlightServer.builder(allocator, location,
                new CookbookProducer(allocator, location)).build()) {
            try {
                flightServer.start();
                System.out.println("S1: Server (Location): Listening on port " + flightServer.getPort());
            } catch (IOException e) {
                System.exit(1);
            }

            // Client
            try (FlightClient flightClient = FlightClient.builder(allocator, location).build()) {
                System.out.println("C1: Client (Location): Connected to " + location.getUri());

                // Populate data
                Schema schema = new Schema(Arrays.asList(
                        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
                try(VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
                    VarCharVector varCharVector = (VarCharVector) vectorSchemaRoot.getVector("name")) {
                    varCharVector.allocateNew(3);
                    varCharVector.set(0, "Ronald".getBytes());
                    varCharVector.set(1, "David".getBytes());
                    varCharVector.set(2, "Francisco".getBytes());
                    vectorSchemaRoot.setRowCount(3);
                    FlightClient.ClientStreamListener listener = flightClient.startPut(
                            FlightDescriptor.path("profiles"),
                            vectorSchemaRoot, new AsyncPutListener());
                    listener.putNext();
                    varCharVector.set(0, "Manuel".getBytes());
                    varCharVector.set(1, "Felipe".getBytes());
                    varCharVector.set(2, "JJ".getBytes());
                    vectorSchemaRoot.setRowCount(3);
                    listener.putNext();
                    listener.completed();
                    listener.getResult();
                    System.out.println("C2: Client (Populate Data): Wrote 2 batches with 3 rows each");
                }

                // Get metadata information
                FlightInfo flightInfo = flightClient.getInfo(FlightDescriptor.path("profiles"));
                System.out.println("C3: Client (Get Metadata): " + flightInfo);

                // Get data information
                try(FlightStream flightStream = flightClient.getStream(new Ticket(
                        FlightDescriptor.path("profiles").getPath().get(0).getBytes(StandardCharsets.UTF_8)))) {
                    int batch = 0;
                    try (VectorSchemaRoot vectorSchemaRootReceived = flightStream.getRoot()) {
                        System.out.println("C4: Client (Get Stream):");
                        while (flightStream.next()) {
                            batch++;
                            System.out.println("Client Received batch #" + batch + ", Data:");
                            System.out.print(vectorSchemaRootReceived.contentToTSVString());
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // Get all metadata information
                Iterable<FlightInfo> flightInfosBefore = flightClient.listFlights(Criteria.ALL);
                System.out.print("C5: Client (List Flights Info): ");
                flightInfosBefore.forEach(t -> System.out.println(t));

                // Do delete action
                Iterator<Result> deleteActionResult = flightClient.doAction(new Action("DELETE",
                        FlightDescriptor.path("profiles").getPath().get(0).getBytes(StandardCharsets.UTF_8)));
                while (deleteActionResult.hasNext()) {
                    Result result = deleteActionResult.next();
                    System.out.println("C6: Client (Do Delete Action): " +
                            new String(result.getBody(), StandardCharsets.UTF_8));
                }

                // Get all metadata information (to validate detele action)
                Iterable<FlightInfo> flightInfos = flightClient.listFlights(Criteria.ALL);
                flightInfos.forEach(t -> System.out.println(t));
                System.out.println("C7: Client (List Flights Info): After delete - No records");

                // Server shut down
                flightServer.shutdown();
                System.out.println("C8: Server shut down successfully");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

.. testoutput::

    S1: Server (Location): Listening on port 33333
    C1: Client (Location): Connected to grpc+tcp://0.0.0.0:33333
    C2: Client (Populate Data): Wrote 2 batches with 3 rows each
    C3: Client (Get Metadata): FlightInfo{schema=Schema<name: Utf8>, descriptor=profiles, endpoints=[FlightEndpoint{locations=[Location{uri=grpc+tcp://0.0.0.0:33333}], ticket=org.apache.arrow.flight.Ticket@58871b0a}], bytes=-1, records=6}
    C4: Client (Get Stream):
    Client Received batch #1, Data:
    name
    Ronald
    David
    Francisco
    Client Received batch #2, Data:
    name
    Manuel
    Felipe
    JJ
    C5: Client (List Flights Info): FlightInfo{schema=Schema<name: Utf8>, descriptor=profiles, endpoints=[FlightEndpoint{locations=[Location{uri=grpc+tcp://0.0.0.0:33333}], ticket=org.apache.arrow.flight.Ticket@58871b0a}], bytes=-1, records=6}
    C6: Client (Do Delete Action): Delete completed
    C7: Client (List Flights Info): After delete - No records
    C8: Server shut down successfully

Let explain our code in more detail.

Start Flight Server
*******************

First, we'll start our server:

.. code-block:: java

    try(FlightServer flightServer = FlightServer.builder(allocator, location,
            new CookbookProducer(allocator, location)).build()) {
        try {
            flightServer.start();
            System.out.println("S1: Server (Location): Listening on port " + flightServer.getPort());
        } catch (IOException e) {
            e.printStackTrace();
        }

.. code-block:: shell

    S1: Server (Location): Listening on port 33333

Connect to Flight Server
************************

We can then create a client and connect to the server:

.. code-block:: java

    try (FlightClient flightClient = FlightClient.builder(allocator, location).build()) {
        System.out.println("C1: Client (Location): Connected to " + location.getUri());

.. code-block:: shell

    C1: Client (Location): Connected to grpc+tcp://0.0.0.0:33333

Put Data
********

First, we'll create and upload a vector schema root, which will get stored in a
memory by the server.

.. code-block:: java

    // Server
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        List<ArrowRecordBatch> batches = new ArrayList<>();
        return () -> {
            long rows = 0;
            VectorUnloader unloader;
            while (flightStream.next()) {
                unloader = new VectorUnloader(flightStream.getRoot());
                try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
                    batches.add(arb);
                    rows += flightStream.getRoot().getRowCount();
                }
            }
            Dataset dataset = new Dataset(batches, flightStream.getSchema(), rows);
            datasets.put(flightStream.getDescriptor(), dataset);
            ackStream.onCompleted();
        };
    }

    // Client
    Schema schema = new Schema(Arrays.asList(
            new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
    try(VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
        VarCharVector varCharVector = (VarCharVector) vectorSchemaRoot.getVector("name")) {
        varCharVector.allocateNew(3);
        varCharVector.set(0, "Ronald".getBytes());
        varCharVector.set(1, "David".getBytes());
        varCharVector.set(2, "Francisco".getBytes());
        vectorSchemaRoot.setRowCount(3);
        FlightClient.ClientStreamListener listener = flightClient.startPut(
                FlightDescriptor.path("profiles"),
                vectorSchemaRoot, new AsyncPutListener());
        listener.putNext();
        varCharVector.set(0, "Manuel".getBytes());
        varCharVector.set(1, "Felipe".getBytes());
        varCharVector.set(2, "JJ".getBytes());
        vectorSchemaRoot.setRowCount(3);
        listener.putNext();
        listener.completed();
        listener.getResult();
        System.out.println("C2: Client (Populate Data): Wrote 2 batches with 3 rows each");
    }

.. code-block:: shell

    C2: Client (Populate Data): Wrote 2 batches with 3 rows each

Get Metadata
************

Once we do so, we can retrieve the metadata for that dataset.

.. code-block:: java

    // Server
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        FlightEndpoint flightEndpoint = new FlightEndpoint(
                new Ticket(descriptor.getPath().get(0).getBytes(StandardCharsets.UTF_8)), location);
        return new FlightInfo(
                datasets.get(descriptor).getSchema(),
                descriptor,
                Collections.singletonList(flightEndpoint),
                /*bytes=*/-1,
                datasets.get(descriptor).getRows()
        );
    }

    // Client
    FlightInfo flightInfo = flightClient.getInfo(FlightDescriptor.path("profiles"));
    System.out.println("C3: Client (Get Metadata): " + flightInfo);

.. code-block:: shell

    C3: Client (Get Metadata): FlightInfo{schema=Schema<name: Utf8>, descriptor=profiles, endpoints=[FlightEndpoint{locations=[Location{uri=grpc+tcp://0.0.0.0:33333}], ticket=org.apache.arrow.flight.Ticket@58871b0a}], bytes=-1, records=6}

Get Data
********

And get the data back:

.. code-block:: java

    // Server
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        FlightDescriptor flightDescriptor = FlightDescriptor.path(
                new String(ticket.getBytes(), StandardCharsets.UTF_8));
        Dataset dataset = this.datasets.get(flightDescriptor);
        if (dataset == null) {
            throw CallStatus.NOT_FOUND.withDescription("Unknown descriptor").toRuntimeException();
        } else {
            VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(
                    this.datasets.get(flightDescriptor).getSchema(), allocator);
            listener.start(vectorSchemaRoot);
            for (ArrowRecordBatch arrowRecordBatch : this.datasets.get(flightDescriptor).getBatches()) {
                VectorLoader loader = new VectorLoader(vectorSchemaRoot);
                loader.load(arrowRecordBatch.cloneWithTransfer(allocator));
                listener.putNext();
            }
            listener.completed();
        }
    }

    // Client
    try(FlightStream flightStream = flightClient.getStream(new Ticket(
            FlightDescriptor.path("profiles").getPath().get(0).getBytes(StandardCharsets.UTF_8)))) {
        int batch = 0;
        try (VectorSchemaRoot vectorSchemaRootReceived = flightStream.getRoot()) {
            System.out.println("C4: Client (Get Stream):");
            while (flightStream.next()) {
                batch++;
                System.out.println("Client Received batch #" + batch + ", Data:");
                System.out.print(vectorSchemaRootReceived.contentToTSVString());
            }
        }
    } catch (Exception e) {
        e.printStackTrace();
    }

.. code-block:: shell

    C4: Client (Get Stream):
    Client Received batch #1, Data:
    name
    Ronald
    David
    Francisco
    Client Received batch #2, Data:
    name
    Manuel
    Felipe
    JJ

Delete data
***********

Then, we'll delete the dataset:

.. code-block:: java

    // Server
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        FlightDescriptor flightDescriptor = FlightDescriptor.path(
                new String(action.getBody(), StandardCharsets.UTF_8));
        switch (action.getType()) {
            case "DELETE":
                if (datasets.remove(flightDescriptor) != null) {
                    Result result = new Result("Delete completed".getBytes(StandardCharsets.UTF_8));
                    listener.onNext(result);
                } else {
                    Result result = new Result("Delete not completed. Reason: Key did not exist."
                            .getBytes(StandardCharsets.UTF_8));
                    listener.onNext(result);
                }
                listener.onCompleted();
        }
    }

    // Client
    Iterator<Result> deleteActionResult = flightClient.doAction(new Action("DELETE",
            FlightDescriptor.path("profiles").getPath().get(0).getBytes(StandardCharsets.UTF_8)));
    while (deleteActionResult.hasNext()) {
        Result result = deleteActionResult.next();
        System.out.println("C6: Client (Do Delete Action): " +
                new String(result.getBody(), StandardCharsets.UTF_8));
    }

.. code-block:: shell

    C6: Client (Do Delete Action): Delete completed

Validate Delete Data
********************

And confirm that it's been deleted:

.. code-block:: java

    // Server
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        datasets.forEach((k, v) -> { listener.onNext(getFlightInfo(null, k)); });
        listener.onCompleted();
    }

    // Client
    Iterable<FlightInfo> flightInfos = flightClient.listFlights(Criteria.ALL);
    flightInfos.forEach(t -> System.out.println(t));
    System.out.println("C7: Client (List Flights Info): After delete - No records");

.. code-block:: shell

    C7: Client (List Flights Info): After delete - No records

Stop Flight Server
******************

.. code-block:: java

    // Server
    flightServer.shutdown();
    System.out.println("C8: Server shut down successfully");

.. code-block:: shell

    C8: Server shut down successfully

_`Arrow Flight RPC`: https://arrow.apache.org/docs/format/Flight.html