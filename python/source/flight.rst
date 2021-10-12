============
Arrow Flight
============

Recipes related to leveraging Arrow Flight protocol

.. contents::

Using an Arrow Flight RPC server
================================

Suppose you want to implement a service that can store, send and receive
Parquet files using the Arrow Flight protocol, 
``pyarrow`` provides an implementation framework in :mod:`pyarrow.flight` 
and particularly through the :class:`pyarrow.flight.FlightServerBase` class.

.. testcode::

    import pathlib
    import threading

    import pyarrow as pa
    import pyarrow.flight
    import pyarrow.parquet


    class FlightServer(pa.flight.FlightServerBase):

        def __init__(self, location="grpc://0.0.0.0:8815", 
                    repo=pathlib.Path("./datasets"), **kwargs):
            super(FlightServer, self).__init__(location, **kwargs)
            self._location = location
            self._repo = repo

        def _make_flight_info(self, dataset):
            dataset_path = self._repo / dataset
            schema = pa.parquet.read_schema(dataset_path)
            metadata = pa.parquet.read_metadata(dataset_path)
            descriptor = pa.flight.FlightDescriptor.for_path(
                dataset.encode('utf-8')
            )
            endpoints = [pa.flight.FlightEndpoint(dataset, [self._location])]
            return pyarrow.flight.FlightInfo(schema,
                                            descriptor, 
                                            endpoints,
                                            metadata.num_rows, 
                                            metadata.serialized_size)

        def list_flights(self, context, criteria):
            for dataset in self._repo.iterdir():
                yield self._make_flight_info(dataset.name)

        def get_flight_info(self, context, descriptor):
            return self._make_flight_info(descriptor.path[0].decode('utf-8'))

        def do_put(self, context, descriptor, reader, writer):
            dataset = descriptor.path[0].decode('utf-8')
            dataset_path = self._repo / dataset
            data_table = reader.read_all()
            pa.parquet.write_table(data_table, dataset_path)

        def do_get(self, context, ticket):
            dataset = ticket.ticket.decode('utf-8')
            dataset_path = self._repo / dataset
            return pa.flight.RecordBatchStream(pa.parquet.read_table(dataset_path))

        def list_actions(self, context):
            return [
                ("drop_dataset", "Delete a dataset."),
            ]

        def do_action(self, context, action):
            if action.type == "drop_dataset":
                self.do_drop_dataset(action.body.to_pybytes().decode('utf-8'))
            else:
                raise NotImplementedError

        def do_drop_dataset(self, dataset):
            dataset_path = self._repo / dataset
            dataset_path.unlink()

The example server exposes :meth:`pyarrow.flight.FlightServerBase.list_flights`
which is the method in charge of returning the list of data streams available
for fetching.

Likewise, :meth:`pyarrow.flight.FlightServerBase.get_flight_info` provides
the information regarding a single specific data stream.

Then we expose :meth:`pyarrow.flight.FlightServerBase.do_get` which is in charge
of actually fetching the exposed data streams and sending them to the client.

Allowing to list and dowload data streams would be pretty useless if we didn't
expose a way to create them, this is the responsability of
:meth:`pyarrow.flight.FlightServerBase.do_put` which is in charge of receiving
new data from the client and dealing with it (in this case saving it
into a parquet file)

This are the most common Arrow Flight requests, if we need to add more
functionalities, we can do so using custom actions.

In the previous example a ``drop_dataset`` custom action is added. 
All custom actions are executed through the 
:meth:`pyarrow.flight.FlightServerBase.do_action` method, thus it's up to
the server subclass to dispatch them properly. In this case we invoke
the `do_drop_dataset` method when the `action.type` is the one we expect.

Our server can then be started with 
:meth:`pyarrow.flight.FlightServerBase.serve`

.. code-block::

    if __name__ == '__main__':
        server = FlightServer()
        server._repo.mkdir(exist_ok=True)
        server.serve()

.. testcode::
    :hide:

    # Code block to start for real a server in background
    # and wait for it to be available.
    # Previous code block is just to show to user how to start it.
    server = FlightServer()
    server._repo.mkdir(exist_ok=True)
    t = threading.Thread(target=server.serve)
    t.start()

    pa.flight.connect("grpc://0.0.0.0:8815").wait_for_available()

Once the server is started we can build a client to perform
requests to it

.. testcode::

    import pyarrow as pa
    import pyarrow.flight

    client = pa.flight.connect("grpc://0.0.0.0:8815")

We can create a new table and upload it so that it gets stored
in a new parquet file:

.. testcode::

    # Upload a new dataset
    data_table = pa.table(
        [["Mario", "Luigi", "Peach"]],
        names=["Character"]
    )
    upload_descriptor = pa.flight.FlightDescriptor.for_path("uploaded.parquet")
    writer, _ = client.do_put(upload_descriptor, data_table.schema)
    writer.write_table(data_table)
    writer.close()

Once uploaded we should be able to retrieve the metadata for our
newly uploaded table:

.. testcode::

    # Retrieve metadata of newly uploaded dataset
    flight = client.get_flight_info(upload_descriptor)
    descriptor = flight.descriptor
    print("Path:", descriptor.path[0].decode('utf-8'), "Rows:", flight.total_records, "Size:", flight.total_bytes)
    print("=== Schema ===")
    print(flight.schema)
    print("==============")

.. testoutput::

    Path: uploaded.parquet Rows: 3 Size: ...
    === Schema ===
    Character: string
    ==============

And we can fetch the content of the dataset:

.. testcode::

    # Read content of the dataset
    reader = client.do_get(flight.endpoints[0].ticket)
    read_table = reader.read_all()
    print(read_table.to_pandas().head())

.. testoutput::

      Character
    0     Mario
    1     Luigi
    2     Peach

Once we finished we can invoke our custom action to delete the
dataset we newly uploaded:

.. testcode::

    # Drop the newly uploaded dataset
    client.do_action(pa.flight.Action("drop_dataset", "uploaded.parquet".encode('utf-8')))

.. testcode::
    :hide:

    # Deal with a bug in do_action, see ARROW-14255
    # can be removed once 6.0.0 is released.
    try:
        list(client.do_action(pa.flight.Action("drop_dataset", "uploaded.parquet".encode('utf-8'))))
    except:
        pass

To confirm our dataset was deleted,
we might list all parquet files that are currently stored by the server:

.. testcode::

    # List existing datasets.
    for flight in client.list_flights():
        descriptor = flight.descriptor
        print("Path:", descriptor.path[0].decode('utf-8'), "Rows:", flight.total_records, "Size:", flight.total_bytes)
        print("=== Schema ===")
        print(flight.schema)
        print("==============")
        print("")

.. testcode::
    :hide:

    # Shutdown the server
    server.shutdown()

