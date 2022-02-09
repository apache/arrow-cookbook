============
Arrow Flight
============

Recipes related to leveraging Arrow Flight protocol

.. contents::

Simple Parquet storage service with Arrow Flight
================================================

Suppose you want to implement a service that can store, send and receive
Parquet files using the Arrow Flight protocol,
``pyarrow`` provides an implementation framework in :mod:`pyarrow.flight`
and particularly through the :class:`pyarrow.flight.FlightServerBase` class.

.. testcode::

    import pathlib

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
    import tempfile
    repo = tempfile.TemporaryDirectory(prefix="arrow-cookbook-flight")
    server = FlightServer(repo=pathlib.Path(repo.name))

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
    repo.cleanup()

Streaming Parquet Storage Service
=================================

We can improve the Parquet storage service and avoid holding entire datasets in
memory by streaming data. Flight readers and writers, like others in PyArrow,
can be iterated through, so let's update the server from before to take
advantage of this:

.. testcode::

   import pathlib

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
           # Read the uploaded data and write to Parquet incrementally
           with dataset_path.open("wb") as sink:
               with pa.parquet.ParquetWriter(sink, reader.schema) as writer:
                   for chunk in reader:
                       writer.write_table(pa.Table.from_batches([chunk.data]))

       def do_get(self, context, ticket):
           dataset = ticket.ticket.decode('utf-8')
           # Stream data from a file
           dataset_path = self._repo / dataset
           reader = pa.parquet.ParquetFile(dataset_path)
           return pa.flight.GeneratorStream(
               reader.schema_arrow, reader.iter_batches())

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

First, we've modified :meth:`pyarrow.flight.FlightServerBase.do_put`. Instead
of reading all the uploaded data into a :class:`pyarrow.Table` before writing,
we instead iterate through each batch as it comes and add it to a Parquet file.

Then, we've modified :meth:`pyarrow.flight.FlightServerBase.do_get` to stream
data to the client. This uses :class:`pyarrow.flight.GeneratorStream`, which
takes a schema and any iterable or iterator. Flight then iterates through and
sends each record batch to the client, allowing us to handle even large Parquet
files that don't fit into memory.

While GeneratorStream has the advantage that it can stream data, that means
Flight must call back into Python for each record batch to send. In contrast,
RecordBatchStream requires that all data is in-memory up front, but once
created, all data transfer is handled purely in C++, without needing to call
Python code.

Let's give the server a spin. As before, we'll start the server:

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
    import tempfile
    repo = tempfile.TemporaryDirectory(prefix="arrow-cookbook-flight")
    server = FlightServer(repo=pathlib.Path(repo.name))

    pa.flight.connect("grpc://0.0.0.0:8815").wait_for_available()

We create a client, and this time, we'll write batches to the writer, as if we
had a stream of data instead of a table in memory:

.. testcode::

   import pyarrow as pa
   import pyarrow.flight

   client = pa.flight.connect("grpc://0.0.0.0:8815")

   # Upload a new dataset
   NUM_BATCHES = 1024
   ROWS_PER_BATCH = 4096
   upload_descriptor = pa.flight.FlightDescriptor.for_path("streamed.parquet")
   batch = pa.record_batch([
       pa.array(range(ROWS_PER_BATCH)),
   ], names=["ints"])
   writer, _ = client.do_put(upload_descriptor, batch.schema)
   with writer:
       for _ in range(NUM_BATCHES):
           writer.write_batch(batch)

As before, we can then read it back. Again, we'll read each batch from the
stream as it arrives, instead of reading them all into a table:

.. testcode::

   # Read content of the dataset
   flight = client.get_flight_info(upload_descriptor)
   reader = client.do_get(flight.endpoints[0].ticket)
   total_rows = 0
   for chunk in reader:
       total_rows += chunk.data.num_rows
   print("Got", total_rows, "rows total, expected", NUM_BATCHES * ROWS_PER_BATCH)

.. testoutput::

   Got 4194304 rows total, expected 4194304

.. testcode::
    :hide:

    # Shutdown the server
    server.shutdown()
    repo.cleanup()

Authentication with user/password
=================================

Often, services need a way to authenticate the user and identify who
they are. Flight provides :doc:`several ways to implement
authentication <pyarrow:format/Flight>`; the simplest uses a
user-password scheme. At startup, the client authenticates itself with
the server using a username and password. The server returns an
authorization token to include on future requests.

.. warning:: Authentication should only be used over a secure encrypted
             channel, i.e. TLS should be enabled.

.. note:: While the scheme is described as "`(HTTP) basic
          authentication`_", it does not actually implement HTTP
          authentication (RFC 7325) per se.

While Flight provides some interfaces to implement such a scheme, the
server must provide the actual implementation, as demonstrated
below. **The implementation here is not secure and is provided as a
minimal example only.**

.. testcode::

   import base64
   import secrets

   import pyarrow as pa
   import pyarrow.flight


   class EchoServer(pa.flight.FlightServerBase):
       """A simple server that just echoes any requests from DoAction."""

       def do_action(self, context, action):
           return [action.type.encode("utf-8"), action.body]


   class BasicAuthServerMiddlewareFactory(pa.flight.ServerMiddlewareFactory):
       """
       Middleware that implements username-password authentication.

       Parameters
       ----------
       creds: Dict[str, str]
           A dictionary of username-password values to accept.
       """

       def __init__(self, creds):
           self.creds = creds
           # Map generated bearer tokens to users
           self.tokens = {}

       def start_call(self, info, headers):
           """Validate credentials at the start of every call."""
           # Search for the authentication header (case-insensitive)
           auth_header = None
           for header in headers:
               if header.lower() == "authorization":
                   auth_header = headers[header][0]
                   break

           if not auth_header:
               raise pa.flight.FlightUnauthenticatedError("No credentials supplied")

           # The header has the structure "AuthType TokenValue", e.g.
           # "Basic <encoded username+password>" or "Bearer <random token>".
           auth_type, _, value = auth_header.partition(" ")

           if auth_type == "Basic":
               # Initial "login". The user provided a username/password
               # combination encoded in the same way as HTTP Basic Auth.
               decoded = base64.b64decode(value).decode("utf-8")
               username, _, password = decoded.partition(':')
               if not password or password != self.creds.get(username):
                   raise pa.flight.FlightUnauthenticatedError("Unknown user or invalid password")
               # Generate a secret, random bearer token for future calls.
               token = secrets.token_urlsafe(32)
               self.tokens[token] = username
               return BasicAuthServerMiddleware(token)
           elif auth_type == "Bearer":
               # An actual call. Validate the bearer token.
               username = self.tokens.get(value)
               if username is None:
                   raise pa.flight.FlightUnauthenticatedError("Invalid token")
               return BasicAuthServerMiddleware(value)

           raise pa.flight.FlightUnauthenticatedError("No credentials supplied")


   class BasicAuthServerMiddleware(pa.flight.ServerMiddleware):
       """Middleware that implements username-password authentication."""

       def __init__(self, token):
           self.token = token

       def sending_headers(self):
           """Return the authentication token to the client."""
           return {"authorization": f"Bearer {self.token}"}


   class NoOpAuthHandler(pa.flight.ServerAuthHandler):
       """
       A handler that implements username-password authentication.

       This is required only so that the server will respond to the internal
       Handshake RPC call, which the client calls when authenticate_basic_token
       is called. Otherwise, it should be a no-op as the actual authentication is
       implemented in middleware.
       """

       def authenticate(self, outgoing, incoming):
           pass

       def is_valid(self, token):
           return ""

We can then start the server:

.. code-block::

    if __name__ == '__main__':
        server = EchoServer(
            auth_handler=NoOpAuthHandler(),
            location="grpc://0.0.0.0:8816",
            middleware={
                "basic": BasicAuthServerMiddlewareFactory({
                    "test": "password",
                })
            },
        )
        server.serve()

.. testcode::
    :hide:

    # Code block to start for real a server in background
    # and wait for it to be available.
    # Previous code block is just to show to user how to start it.
    import threading
    server = EchoServer(
        auth_handler=NoOpAuthHandler(),
        location="grpc://0.0.0.0:8816",
        middleware={
            "basic": BasicAuthServerMiddlewareFactory({
                "test": "password",
            })
        },
    )
    t = threading.Thread(target=server.serve)
    t.start()

Then, we can make a client and log in:

.. testcode::

   import pyarrow as pa
   import pyarrow.flight

   client = pa.flight.connect("grpc://0.0.0.0:8816")

   token_pair = client.authenticate_basic_token(b'test', b'password')
   print(token_pair)

.. testoutput::

   (b'authorization', b'Bearer ...')

For future calls, we include the authentication token with the call:

.. testcode::

   action = pa.flight.Action("echo", b"Hello, world!")
   options = pa.flight.FlightCallOptions(headers=[token_pair])
   for response in client.do_action(action=action, options=options):
       print(response.body.to_pybytes())

.. testoutput::

   b'echo'
   b'Hello, world!'

If we fail to do so, we get an authentication error:

.. testcode::

   try:
       list(client.do_action(action=action))
   except pa.flight.FlightUnauthenticatedError as e:
       print("Unauthenticated:", e)
   else:
       raise RuntimeError("Expected call to fail")

.. testoutput::

   Unauthenticated: No credentials supplied. Detail: Unauthenticated

Or if we use the wrong credentials on login, we also get an error:

.. testcode::

   try:
       client.authenticate_basic_token(b'invalid', b'password')
   except pa.flight.FlightUnauthenticatedError as e:
       print("Unauthenticated:", e)
   else:
       raise RuntimeError("Expected call to fail")

.. testoutput::

   Unauthenticated: Unknown user or invalid password. Detail: Unauthenticated

.. testcode::
    :hide:

    # Shutdown the server
    server.shutdown()

.. _(HTTP) basic authentication: https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#basic_authentication_scheme
