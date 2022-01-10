package usecase.flight;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;

public class ArrowFlightServer {
    public ArrowFlightServer() {
    }

    public void start(int numberOfArrowFlightServerInstances) throws InterruptedException, IOException {
        BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        FlightServer server = FlightServer.builder(
                allocator,
                Location.forGrpcInsecure("localhost", 33443),
                new ArrowFlightServerProducer(
                        numberOfArrowFlightServerInstances
                )
        ).build();
        server.start();
        server.awaitTermination();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        new ArrowFlightServer().start(10);
    }
}