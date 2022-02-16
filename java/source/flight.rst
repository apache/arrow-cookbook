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

This section contains a number of recipes for working with Arrow
Flight. For moreb about Flight.

.. contents::

Simple VectorSchemaRoot storage service with Arrow Flight
=========================================================

We'll implement a service to transfer batches flow through VectorSchemaRoot.

Flight Client and Server
************************

.. code-block:: java

    import org.apache.arrow.flight.Action;
    import org.apache.arrow.flight.AsyncPutListener;
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
    import java.util.HashMap;
    import java.util.Iterator;
    import java.util.List;
    import java.util.Map;

    class DataInMemory {
        private List<ArrowRecordBatch> listArrowRecordBatch;
        private Schema schema;
        private Long rows;
        public DataInMemory(List<ArrowRecordBatch> listArrowRecordBatch, Schema schema, Long rows) {
            this.listArrowRecordBatch = listArrowRecordBatch;
            this.schema = schema;
            this.rows = rows;
        }
        public List<ArrowRecordBatch> getListArrowRecordBatch() {
            return listArrowRecordBatch;
        }
        public Schema getSchema() {
            return schema;
        }
        public Long getRows() {
            return rows;
        }
    }

    // Server
    Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
    Map<FlightDescriptor, DataInMemory> dataInMemory = new HashMap<>();
    Map<String, DataInMemory> mapPojoFlightDataInMemory = new HashMap<>();
    List<ArrowRecordBatch> listArrowRecordBatch = new ArrayList<>();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)){
        FlightServer flightServer = FlightServer.builder(allocator, location, new NoOpFlightProducer(){
            @Override
            public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
                return () -> {
                    long rows = 0;
                    while (flightStream.next()) {
                        VectorUnloader unloader = new VectorUnloader(flightStream.getRoot());
                        try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
                            // Retain data information
                            listArrowRecordBatch.add(arb);
                            rows = rows + flightStream.getRoot().getRowCount();
                        }
                    }
                    long finalRows = rows;
                    DataInMemory pojoFlightDataInMemory = new DataInMemory(listArrowRecordBatch, flightStream.getSchema(), finalRows);
                    dataInMemory.put(flightStream.getDescriptor(), pojoFlightDataInMemory);
                    ackStream.onCompleted();
                };
            }

            @Override
            public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
                FlightDescriptor flightDescriptor = FlightDescriptor.path(new String(ticket.getBytes(), StandardCharsets.UTF_8)); // Recover data for key configured
                if(dataInMemory.containsKey(flightDescriptor)){
                    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(dataInMemory.get(flightDescriptor).getSchema(), allocator);
                    listener.start(vectorSchemaRoot);
                    for(ArrowRecordBatch arrowRecordBatch : dataInMemory.get(flightDescriptor).getListArrowRecordBatch()){
                        vectorSchemaRoot.allocateNew();
                        VectorLoader loader = new VectorLoader(vectorSchemaRoot);
                        loader.load(arrowRecordBatch.cloneWithTransfer(allocator));
                        listener.putNext();
                    }
                    vectorSchemaRoot.clear();
                    listener.completed();
                }
            }

            @Override
            public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
                FlightDescriptor flightDescriptor = FlightDescriptor.path(new String(action.getBody(), StandardCharsets.UTF_8)); // For recover data for key configured
                if(dataInMemory.containsKey(flightDescriptor)) {
                    switch (action.getType()) {
                        case "DELETE":
                            dataInMemory.remove(flightDescriptor);
                            Result result = new Result("Delete completed".getBytes(StandardCharsets.UTF_8));
                            listener.onNext(result);
                    }
                    listener.onCompleted();
                }
            }

            @Override
            public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
                if(!dataInMemory.containsKey(descriptor)){
                    throw new IllegalStateException("Unknown descriptor.");
                }
                return new FlightInfo(
                        dataInMemory.get(descriptor).getSchema(),
                        descriptor,
                        Collections.singletonList(new FlightEndpoint(new Ticket(descriptor.getPath().get(0).getBytes(StandardCharsets.UTF_8)), location)), // Configure a key to map back and forward your data using Ticket argument
                        allocator.getAllocatedMemory(),
                        dataInMemory.get(descriptor).getRows()
                );
            }

            @Override
            public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
                dataInMemory.forEach((k, v) -> {
                            FlightInfo flightInfo = getFlightInfo(null, k);
                            listener.onNext(flightInfo);
                        }
                );
                listener.onCompleted();
            }
        }).build();
        try {
            flightServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Client
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)){
        // Populate data
        FlightClient flightClient = FlightClient.builder(allocator, location).build();
        Schema schema = new Schema(Arrays.asList( new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
        VarCharVector varCharVector = (VarCharVector) vectorSchemaRoot.getVector("name");
        varCharVector.allocateNew(3);
        varCharVector.set(0, "Ronald".getBytes());
        varCharVector.set(1, "David".getBytes());
        varCharVector.set(2, "Francisco".getBytes());
        varCharVector.setValueCount(3);
        vectorSchemaRoot.setRowCount(3);
        FlightClient.ClientStreamListener listener = flightClient.startPut(FlightDescriptor.path("profiles"), vectorSchemaRoot, new AsyncPutListener());
        listener.putNext();
        vectorSchemaRoot.allocateNew();
        varCharVector.set(0, "Manuel".getBytes());
        varCharVector.set(1, "Felipe".getBytes());
        varCharVector.set(2, "JJ".getBytes());
        varCharVector.setValueCount(3);
        vectorSchemaRoot.setRowCount(3);
        listener.putNext();
        vectorSchemaRoot.clear();
        listener.completed();
        listener.getResult();

        // Get all metadata information
        Iterable<FlightInfo> flightInfosBefore = flightClient.listFlights(Criteria.ALL);
        System.out.print("List Flights Info: ");
        flightInfosBefore.forEach(t -> System.out.println(t));

        // Get data information
        FlightStream flightStream = flightClient.getStream(new Ticket(FlightDescriptor.path("profiles").getPath().get(0).getBytes(StandardCharsets.UTF_8)));
        int batch = 0;
        VectorSchemaRoot vectorSchemaRootReceived = flightStream.getRoot();
        while(flightStream.next()){
            batch++;
            System.out.println("Received batch #" + batch + ", Data:");
            System.out.print(vectorSchemaRootReceived.contentToTSVString());
            vectorSchemaRootReceived.clear();
        }

        // Do delete action
        Iterator<Result> deleteActionResult = flightClient.doAction(new Action("DELETE", FlightDescriptor.path("profiles").getPath().get(0).getBytes(StandardCharsets.UTF_8) ));
        while(deleteActionResult.hasNext()){
            Result result = deleteActionResult.next();
            System.out.println("Do Delete Action: " + new String(result.getBody(), StandardCharsets.UTF_8));
        }

        // Get all metadata information (to validate detele action)
        Iterable<FlightInfo> flightInfos = flightClient.listFlights(Criteria.ALL);
        flightInfos.forEach(t -> System.out.println(t));
        System.out.println("List Flights Info (after delete): No records");
    }

Let explain our code in more detail.

Start Flight Server
*******************

First, we'll start our server:

.. testcode::

    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.NoOpFlightProducer;
    import org.apache.arrow.memory.RootAllocator;
    // Server
    try (final RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE)){
        FlightServer flightServer = FlightServer.builder(rootAllocator, Location.forGrpcInsecure("0.0.0.0", 33333), new NoOpFlightProducer() {
        }).build();
        flightServer.start();

        System.out.println("Listening on port " + flightServer.getPort());
    } catch (IOException e) {
        e.printStackTrace();
    }

.. testoutput::

    Listening on port 33333

Connect to Flight Server
************************

We can then create a client and connect to the server:

.. testcode::

    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.NoOpFlightProducer;
    import org.apache.arrow.memory.RootAllocator;
    // Server
    Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
    try (final RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE)){
        FlightServer flightServer = FlightServer.builder(rootAllocator, location, new NoOpFlightProducer() {
        }).build();
        try {
            flightServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    // Client
    try (final RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE)){
        FlightClient flightClient = FlightClient.builder(rootAllocator, location).build();
        System.out.println("Connected to " + location.getUri());
    }

.. testoutput::

    Connected to grpc+tcp://0.0.0.0:33333

Put Data
********

First, we'll create and upload a vector schema root, which will get stored in a
memory by the server.

.. testcode::

    import org.apache.arrow.flight.AsyncPutListener;
    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.FlightDescriptor;
    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.FlightStream;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.NoOpFlightProducer;
    import org.apache.arrow.flight.PutResult;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.VectorUnloader;
    import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.Schema;

    import java.io.IOException;
    import java.util.ArrayList;
    import java.util.Arrays;
    import java.util.HashMap;
    import java.util.List;
    import java.util.Map;

    class DataInMemory {
        private List<ArrowRecordBatch> listArrowRecordBatch;
        private Schema schema;
        private Long rows;
        public DataInMemory(List<ArrowRecordBatch> listArrowRecordBatch, Schema schema, Long rows) {
            this.listArrowRecordBatch = listArrowRecordBatch;
            this.schema = schema;
            this.rows = rows;
        }
        public List<ArrowRecordBatch> getListArrowRecordBatch() {
            return listArrowRecordBatch;
        }
        public Schema getSchema() {
            return schema;
        }
        public Long getRows() {
            return rows;
        }
    }

    // Server
    Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
    Map<FlightDescriptor, DataInMemory> dataInMemory = new HashMap<>();
    Map<String, DataInMemory> mapPojoFlightDataInMemory = new HashMap<>();
    List<ArrowRecordBatch> listArrowRecordBatch = new ArrayList<>();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)){
        FlightServer flightServer = FlightServer.builder(allocator, location, new NoOpFlightProducer(){
            @Override
            public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
                return () -> {
                    long rows = 0;
                    while (flightStream.next()) {
                        VectorUnloader unloader = new VectorUnloader(flightStream.getRoot());
                        try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
                            // Retain data information
                            listArrowRecordBatch.add(arb);
                            rows = rows + flightStream.getRoot().getRowCount();
                        }
                    }
                    long finalRows = rows;
                    DataInMemory pojoFlightDataInMemory = new DataInMemory(listArrowRecordBatch, flightStream.getSchema(), finalRows);
                    dataInMemory.put(flightStream.getDescriptor(), pojoFlightDataInMemory);
                    ackStream.onCompleted();
                };
            }
        }).build();
        try {
            flightServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Client
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)){
        // Populate data
        FlightClient flightClient = FlightClient.builder(allocator, location).build();
        Schema schema = new Schema(Arrays.asList( new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
        VarCharVector varCharVector = (VarCharVector) vectorSchemaRoot.getVector("name");
        varCharVector.allocateNew(3);
        varCharVector.set(0, "Ronald".getBytes());
        varCharVector.set(1, "David".getBytes());
        varCharVector.set(2, "Francisco".getBytes());
        varCharVector.setValueCount(3);
        vectorSchemaRoot.setRowCount(3);
        FlightClient.ClientStreamListener listener = flightClient.startPut(FlightDescriptor.path("profiles"), vectorSchemaRoot, new AsyncPutListener());
        listener.putNext();
        vectorSchemaRoot.allocateNew();
        varCharVector.set(0, "Manuel".getBytes());
        varCharVector.set(1, "Felipe".getBytes());
        varCharVector.set(2, "JJ".getBytes());
        varCharVector.setValueCount(3);
        vectorSchemaRoot.setRowCount(3);
        listener.putNext();
        vectorSchemaRoot.clear();
        listener.completed();
        listener.getResult();
    }

    System.out.println("Wrote 2 batches with 3 rows each");

.. testoutput::

    Wrote 2 batches with 3 rows each

Get Metadata
************

Once we do so, we can retrieve the metadata for that dataset.

.. testcode::

    import org.apache.arrow.flight.AsyncPutListener;
    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.FlightDescriptor;
    import org.apache.arrow.flight.FlightEndpoint;
    import org.apache.arrow.flight.FlightInfo;
    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.FlightStream;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.NoOpFlightProducer;
    import org.apache.arrow.flight.PutResult;
    import org.apache.arrow.flight.Ticket;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VarCharVector;
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
    import java.util.HashMap;
    import java.util.List;
    import java.util.Map;

    class DataInMemory {
        private List<ArrowRecordBatch> listArrowRecordBatch;
        private Schema schema;
        private Long rows;
        public DataInMemory(List<ArrowRecordBatch> listArrowRecordBatch, Schema schema, Long rows) {
            this.listArrowRecordBatch = listArrowRecordBatch;
            this.schema = schema;
            this.rows = rows;
        }
        public List<ArrowRecordBatch> getListArrowRecordBatch() {
            return listArrowRecordBatch;
        }
        public Schema getSchema() {
            return schema;
        }
        public Long getRows() {
            return rows;
        }
    }

    // Server
    Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
    Map<FlightDescriptor, DataInMemory> dataInMemory = new HashMap<>();
    Map<String, DataInMemory> mapPojoFlightDataInMemory = new HashMap<>();
    List<ArrowRecordBatch> listArrowRecordBatch = new ArrayList<>();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)){
        FlightServer flightServer = FlightServer.builder(allocator, location, new NoOpFlightProducer(){
            @Override
            public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
                return () -> {
                    long rows = 0;
                    while (flightStream.next()) {
                        VectorUnloader unloader = new VectorUnloader(flightStream.getRoot());
                        try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
                            // Retain data information
                            listArrowRecordBatch.add(arb);
                            rows = rows + flightStream.getRoot().getRowCount();
                        }
                    }
                    long finalRows = rows;
                    DataInMemory pojoFlightDataInMemory = new DataInMemory(listArrowRecordBatch, flightStream.getSchema(), finalRows);
                    dataInMemory.put(flightStream.getDescriptor(), pojoFlightDataInMemory);
                    ackStream.onCompleted();
                };
            }

            @Override
            public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
                if(!dataInMemory.containsKey(descriptor)){
                    throw new IllegalStateException("Unknown descriptor.");
                }
                return new FlightInfo(
                        dataInMemory.get(descriptor).getSchema(),
                        descriptor,
                        Collections.singletonList(new FlightEndpoint(new Ticket(descriptor.getPath().get(0).getBytes(StandardCharsets.UTF_8)), location)), // Configure a key to map back and forward your data using Ticket argument
                        allocator.getAllocatedMemory(),
                        dataInMemory.get(descriptor).getRows()
                );
            }
        }).build();
        try {
            flightServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Client
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)){
        // Populate data
        FlightClient flightClient = FlightClient.builder(allocator, location).build();
        Schema schema = new Schema(Arrays.asList( new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
        VarCharVector varCharVector = (VarCharVector) vectorSchemaRoot.getVector("name");
        varCharVector.allocateNew(3);
        varCharVector.set(0, "Ronald".getBytes());
        varCharVector.set(1, "David".getBytes());
        varCharVector.set(2, "Francisco".getBytes());
        varCharVector.setValueCount(3);
        vectorSchemaRoot.setRowCount(3);
        FlightClient.ClientStreamListener listener = flightClient.startPut(FlightDescriptor.path("profiles"), vectorSchemaRoot, new AsyncPutListener());
        listener.putNext();
        vectorSchemaRoot.allocateNew();
        varCharVector.set(0, "Manuel".getBytes());
        varCharVector.set(1, "Felipe".getBytes());
        varCharVector.set(2, "JJ".getBytes());
        varCharVector.setValueCount(3);
        vectorSchemaRoot.setRowCount(3);
        listener.putNext();
        vectorSchemaRoot.clear();
        listener.completed();
        listener.getResult();

        // Get metadata information
        FlightInfo flightInfo = flightClient.getInfo(FlightDescriptor.path("profiles"));
        System.out.println(flightInfo);
    }

.. testoutput::

    FlightInfo{schema=Schema<name: Utf8>, descriptor=profiles, endpoints=[FlightEndpoint{locations=[Location{uri=grpc+tcp://0.0.0.0:33333}], ticket=org.apache.arrow.flight.Ticket@58871b0a}], bytes=0, records=6}

Get Data
********

And get the data back:

.. testcode::

    import org.apache.arrow.flight.AsyncPutListener;
    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.FlightDescriptor;
    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.FlightStream;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.NoOpFlightProducer;
    import org.apache.arrow.flight.PutResult;
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
    import java.util.HashMap;
    import java.util.List;
    import java.util.Map;

    class DataInMemory {
        private List<ArrowRecordBatch> listArrowRecordBatch;
        private Schema schema;
        private Long rows;
        public DataInMemory(List<ArrowRecordBatch> listArrowRecordBatch, Schema schema, Long rows) {
            this.listArrowRecordBatch = listArrowRecordBatch;
            this.schema = schema;
            this.rows = rows;
        }
        public List<ArrowRecordBatch> getListArrowRecordBatch() {
            return listArrowRecordBatch;
        }
        public Schema getSchema() {
            return schema;
        }
        public Long getRows() {
            return rows;
        }
    }

    // Server
    Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
    Map<FlightDescriptor, DataInMemory> dataInMemory = new HashMap<>();
    Map<String, DataInMemory> mapPojoFlightDataInMemory = new HashMap<>();
    List<ArrowRecordBatch> listArrowRecordBatch = new ArrayList<>();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)){
        FlightServer flightServer = FlightServer.builder(allocator, location, new NoOpFlightProducer(){
            @Override
            public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
                return () -> {
                    long rows = 0;
                    while (flightStream.next()) {
                        VectorUnloader unloader = new VectorUnloader(flightStream.getRoot());
                        try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
                            // Retain data information
                            listArrowRecordBatch.add(arb);
                            rows = rows + flightStream.getRoot().getRowCount();
                        }
                    }
                    long finalRows = rows;
                    DataInMemory pojoFlightDataInMemory = new DataInMemory(listArrowRecordBatch, flightStream.getSchema(), finalRows);
                    dataInMemory.put(flightStream.getDescriptor(), pojoFlightDataInMemory);
                    ackStream.onCompleted();
                };
            }

            @Override
            public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
                FlightDescriptor flightDescriptor = FlightDescriptor.path(new String(ticket.getBytes(), StandardCharsets.UTF_8)); // Recover data for key configured
                if(dataInMemory.containsKey(flightDescriptor)){
                    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(dataInMemory.get(flightDescriptor).getSchema(), allocator);
                    listener.start(vectorSchemaRoot);
                    for(ArrowRecordBatch arrowRecordBatch : dataInMemory.get(flightDescriptor).getListArrowRecordBatch()){
                        vectorSchemaRoot.allocateNew();
                        VectorLoader loader = new VectorLoader(vectorSchemaRoot);
                        loader.load(arrowRecordBatch.cloneWithTransfer(allocator));
                        listener.putNext();
                    }
                    vectorSchemaRoot.clear();
                    listener.completed();
                }
            }
        }).build();
        try {
            flightServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Client
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)){
        // Populate data
        FlightClient flightClient = FlightClient.builder(allocator, location).build();
        Schema schema = new Schema(Arrays.asList( new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
        VarCharVector varCharVector = (VarCharVector) vectorSchemaRoot.getVector("name");
        varCharVector.allocateNew(3);
        varCharVector.set(0, "Ronald".getBytes());
        varCharVector.set(1, "David".getBytes());
        varCharVector.set(2, "Francisco".getBytes());
        varCharVector.setValueCount(3);
        vectorSchemaRoot.setRowCount(3);
        FlightClient.ClientStreamListener listener = flightClient.startPut(FlightDescriptor.path("profiles"), vectorSchemaRoot, new AsyncPutListener());
        listener.putNext();
        vectorSchemaRoot.allocateNew();
        varCharVector.set(0, "Manuel".getBytes());
        varCharVector.set(1, "Felipe".getBytes());
        varCharVector.set(2, "JJ".getBytes());
        varCharVector.setValueCount(3);
        vectorSchemaRoot.setRowCount(3);
        listener.putNext();
        vectorSchemaRoot.clear();
        listener.completed();
        listener.getResult();

        // Get data information
        FlightStream flightStream = flightClient.getStream(new Ticket(FlightDescriptor.path("profiles").getPath().get(0).getBytes(StandardCharsets.UTF_8)));
        int batch = 0;
        VectorSchemaRoot vectorSchemaRootReceived = flightStream.getRoot();
        while(flightStream.next()){
            batch++;
            System.out.println("Received batch #" + batch + ", Data:");
            System.out.print(vectorSchemaRootReceived.contentToTSVString());
            vectorSchemaRootReceived.clear();
        }
    }

.. testoutput::

    Received batch #1, Data:
    name
    Ronald
    David
    Francisco
    Received batch #2, Data:
    name
    Manuel
    Felipe
    JJ

Delete data
***********

Then, we'll delete the dataset:

.. testcode::

    import org.apache.arrow.flight.Action;
    import org.apache.arrow.flight.AsyncPutListener;
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
    import java.util.HashMap;
    import java.util.Iterator;
    import java.util.List;
    import java.util.Map;

    class DataInMemory {
        private List<ArrowRecordBatch> listArrowRecordBatch;
        private Schema schema;
        private Long rows;
        public DataInMemory(List<ArrowRecordBatch> listArrowRecordBatch, Schema schema, Long rows) {
            this.listArrowRecordBatch = listArrowRecordBatch;
            this.schema = schema;
            this.rows = rows;
        }
        public List<ArrowRecordBatch> getListArrowRecordBatch() {
            return listArrowRecordBatch;
        }
        public Schema getSchema() {
            return schema;
        }
        public Long getRows() {
            return rows;
        }
    }

    // Server
    Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
    Map<FlightDescriptor, DataInMemory> dataInMemory = new HashMap<>();
    Map<String, DataInMemory> mapPojoFlightDataInMemory = new HashMap<>();
    List<ArrowRecordBatch> listArrowRecordBatch = new ArrayList<>();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)){
        FlightServer flightServer = FlightServer.builder(allocator, location, new NoOpFlightProducer(){
            @Override
            public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
                return () -> {
                    long rows = 0;
                    while (flightStream.next()) {
                        VectorUnloader unloader = new VectorUnloader(flightStream.getRoot());
                        try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
                            // Retain data information
                            listArrowRecordBatch.add(arb);
                            rows = rows + flightStream.getRoot().getRowCount();
                        }
                    }
                    long finalRows = rows;
                    DataInMemory pojoFlightDataInMemory = new DataInMemory(listArrowRecordBatch, flightStream.getSchema(), finalRows);
                    dataInMemory.put(flightStream.getDescriptor(), pojoFlightDataInMemory);
                    ackStream.onCompleted();
                };
            }

            @Override
            public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
                FlightDescriptor flightDescriptor = FlightDescriptor.path(new String(action.getBody(), StandardCharsets.UTF_8)); // For recover data for key configured
                if(dataInMemory.containsKey(flightDescriptor)) {
                    switch (action.getType()) {
                        case "DELETE":
                            dataInMemory.remove(flightDescriptor);
                            Result result = new Result("Delete completed".getBytes(StandardCharsets.UTF_8));
                            listener.onNext(result);
                    }
                    listener.onCompleted();
                }
            }

            @Override
            public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
                if(!dataInMemory.containsKey(descriptor)){
                    throw new IllegalStateException("Unknown descriptor.");
                }
                return new FlightInfo(
                        dataInMemory.get(descriptor).getSchema(),
                        descriptor,
                        Collections.singletonList(new FlightEndpoint(new Ticket(descriptor.getPath().get(0).getBytes(StandardCharsets.UTF_8)), location)), // Configure a key to map back and forward your data using Ticket argument
                        allocator.getAllocatedMemory(),
                        dataInMemory.get(descriptor).getRows()
                );
            }

            @Override
            public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
                dataInMemory.forEach((k, v) -> {
                    FlightInfo flightInfo = getFlightInfo(null, k);
                    listener.onNext(flightInfo);
                    }
                );
                listener.onCompleted();
            }
        }).build();
        try {
            flightServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Client
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)){
        // Populate data
        FlightClient flightClient = FlightClient.builder(allocator, location).build();
        Schema schema = new Schema(Arrays.asList( new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
        VarCharVector varCharVector = (VarCharVector) vectorSchemaRoot.getVector("name");
        varCharVector.allocateNew(3);
        varCharVector.set(0, "Ronald".getBytes());
        varCharVector.set(1, "David".getBytes());
        varCharVector.set(2, "Francisco".getBytes());
        varCharVector.setValueCount(3);
        vectorSchemaRoot.setRowCount(3);
        FlightClient.ClientStreamListener listener = flightClient.startPut(FlightDescriptor.path("profiles"), vectorSchemaRoot, new AsyncPutListener());
        listener.putNext();
        vectorSchemaRoot.allocateNew();
        varCharVector.set(0, "Manuel".getBytes());
        varCharVector.set(1, "Felipe".getBytes());
        varCharVector.set(2, "JJ".getBytes());
        varCharVector.setValueCount(3);
        vectorSchemaRoot.setRowCount(3);
        listener.putNext();
        vectorSchemaRoot.clear();
        listener.completed();
        listener.getResult();

        // Get all metadata information
        Iterable<FlightInfo> flightInfosBefore = flightClient.listFlights(Criteria.ALL);
        System.out.print("List Flights Info: ");
        flightInfosBefore.forEach(t -> System.out.println(t));

        // Do delete action
        Iterator<Result> deleteActionResult = flightClient.doAction(new Action("DELETE", FlightDescriptor.path("profiles").getPath().get(0).getBytes(StandardCharsets.UTF_8) ));
        while(deleteActionResult.hasNext()){
            Result result = deleteActionResult.next();
            System.out.println("Do Delete Action: " + new String(result.getBody(), StandardCharsets.UTF_8));
        }
    }

.. testoutput::

    List Flights Info: FlightInfo{schema=Schema<name: Utf8>, descriptor=profiles, endpoints=[FlightEndpoint{locations=[Location{uri=grpc+tcp://0.0.0.0:33333}], ticket=org.apache.arrow.flight.Ticket@58871b0a}], bytes=0, records=6}
    Do Delete Action: Delete completed

Validate Delete Data
********************

And confirm that it's been deleted:

.. testcode::

    import org.apache.arrow.flight.Action;
    import org.apache.arrow.flight.AsyncPutListener;
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
    import java.util.HashMap;
    import java.util.Iterator;
    import java.util.List;
    import java.util.Map;

    class DataInMemory {
        private List<ArrowRecordBatch> listArrowRecordBatch;
        private Schema schema;
        private Long rows;
        public DataInMemory(List<ArrowRecordBatch> listArrowRecordBatch, Schema schema, Long rows) {
            this.listArrowRecordBatch = listArrowRecordBatch;
            this.schema = schema;
            this.rows = rows;
        }
        public List<ArrowRecordBatch> getListArrowRecordBatch() {
            return listArrowRecordBatch;
        }
        public Schema getSchema() {
            return schema;
        }
        public Long getRows() {
            return rows;
        }
    }

    // Server
    Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
    Map<FlightDescriptor, DataInMemory> dataInMemory = new HashMap<>();
    Map<String, DataInMemory> mapPojoFlightDataInMemory = new HashMap<>();
    List<ArrowRecordBatch> listArrowRecordBatch = new ArrayList<>();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)){
        FlightServer flightServer = FlightServer.builder(allocator, location, new NoOpFlightProducer(){
            @Override
            public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
                return () -> {
                    long rows = 0;
                    while (flightStream.next()) {
                        VectorUnloader unloader = new VectorUnloader(flightStream.getRoot());
                        try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
                            // Retain data information
                            listArrowRecordBatch.add(arb);
                            rows = rows + flightStream.getRoot().getRowCount();
                        }
                    }
                    long finalRows = rows;
                    DataInMemory pojoFlightDataInMemory = new DataInMemory(listArrowRecordBatch, flightStream.getSchema(), finalRows);
                    dataInMemory.put(flightStream.getDescriptor(), pojoFlightDataInMemory);
                    ackStream.onCompleted();
                };
            }

            @Override
            public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
                FlightDescriptor flightDescriptor = FlightDescriptor.path(new String(action.getBody(), StandardCharsets.UTF_8)); // For recover data for key configured
                if(dataInMemory.containsKey(flightDescriptor)) {
                    switch (action.getType()) {
                        case "DELETE":
                            dataInMemory.remove(flightDescriptor);
                            Result result = new Result("Delete completed".getBytes(StandardCharsets.UTF_8));
                            listener.onNext(result);
                    }
                    listener.onCompleted();
                }
            }

            @Override
            public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
                if(!dataInMemory.containsKey(descriptor)){
                    throw new IllegalStateException("Unknown descriptor.");
                }
                return new FlightInfo(
                        dataInMemory.get(descriptor).getSchema(),
                        descriptor,
                        Collections.singletonList(new FlightEndpoint(new Ticket(descriptor.getPath().get(0).getBytes(StandardCharsets.UTF_8)), location)), // Configure a key to map back and forward your data using Ticket argument
                        allocator.getAllocatedMemory(),
                        dataInMemory.get(descriptor).getRows()
                );
            }

            @Override
            public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
                dataInMemory.forEach((k, v) -> {
                    FlightInfo flightInfo = getFlightInfo(null, k);
                    listener.onNext(flightInfo);
                    }
                );
                listener.onCompleted();
            }
        }).build();
        try {
            flightServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Client
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)){
        // Populate data
        FlightClient flightClient = FlightClient.builder(allocator, location).build();
        Schema schema = new Schema(Arrays.asList( new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
        VarCharVector varCharVector = (VarCharVector) vectorSchemaRoot.getVector("name");
        varCharVector.allocateNew(3);
        varCharVector.set(0, "Ronald".getBytes());
        varCharVector.set(1, "David".getBytes());
        varCharVector.set(2, "Francisco".getBytes());
        varCharVector.setValueCount(3);
        vectorSchemaRoot.setRowCount(3);
        FlightClient.ClientStreamListener listener = flightClient.startPut(FlightDescriptor.path("profiles"), vectorSchemaRoot, new AsyncPutListener());
        listener.putNext();
        vectorSchemaRoot.allocateNew();
        varCharVector.set(0, "Manuel".getBytes());
        varCharVector.set(1, "Felipe".getBytes());
        varCharVector.set(2, "JJ".getBytes());
        varCharVector.setValueCount(3);
        vectorSchemaRoot.setRowCount(3);
        listener.putNext();
        vectorSchemaRoot.clear();
        listener.completed();
        listener.getResult();

        // Do delete action
        Iterator<Result> deleteActionResult = flightClient.doAction(new Action("DELETE", FlightDescriptor.path("profiles").getPath().get(0).getBytes(StandardCharsets.UTF_8) ));
        while(deleteActionResult.hasNext()){
            Result result = deleteActionResult.next();
            System.out.println("Do Delete Action: " + new String(result.getBody(), StandardCharsets.UTF_8));
        }

        // Get all metadata information
        Iterable<FlightInfo> flightInfos = flightClient.listFlights(Criteria.ALL);
        flightInfos.forEach(t -> System.out.println(t));
        System.out.println("List Flights Info (after delete): No records");
    }

.. testoutput::

    Do Delete Action: Delete completed
    List Flights Info (after delete): No records

