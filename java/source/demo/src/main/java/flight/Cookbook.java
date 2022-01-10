package flight;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public class Cookbook {
    public static void main(String[] args) {
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

        // client creation
        FlightClient client = FlightClient.builder(allocator, Location.forGrpcInsecure("localhost", 22334)).build();


        /**
         * 0.- Lists actions available on the Flight service.
         */
        for (ActionType at : client.listActions()) {
            System.out.println(at.getType());
        }

        /**
         * 1.- Lists flight information.
         */
        Iterable<FlightInfo> listFlights = client.listFlights(Criteria.ALL);
        listFlights.forEach(t -> System.out.println(t));

        /**
         * 2.- Exchange data.
         */

        // use vectorschemaroot created at
        VectorSchemaRoot vectorSchemaRoot = io.Util.createVectorSchemaRoot();

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
         * 3.- Lists flight information again.
         */
        listFlights = client.listFlights(Criteria.ALL);
        listFlights.forEach(t -> System.out.println(t));

        /**
         * 4.- Get info por new path just created
         */

        FlightInfo info = client.getInfo(FlightDescriptor.path("hello"));
        System.out.println("hello");
        System.out.println(info);


        /**
         * 5.- Request data.
         */
//        try (final FlightStream stream = client.getStream(new Ticket(new byte[] {}), CallOptions.timeout(500, TimeUnit.DAYS))) {
        try (final FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket())) {
            while (stream.next()) {
                // do whatever with VectorSchemaRoot response: stream.getRoot()
                System.out.println(stream.getRoot().contentToTSVString());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
