package usecase.flight;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.util.concurrent.TimeUnit;

public class ArrowFlightClientParallel {
    public ArrowFlightClientParallel(int numberOfParallelRequests) {
        executeParallelRequest(numberOfParallelRequests);
    }

    class SimulateClientNodeParallel implements Runnable {
        public void run() {
            long start = System.currentTimeMillis(); // time start
            BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
            FlightClient client = FlightClient.builder(allocator, Location.forGrpcInsecure("localhost", 33443)).build();
            // reqyest data to the server with timeout configuration
            FlightStream stream = client.getStream(new Ticket(new byte[] {}), CallOptions.timeout(500, TimeUnit.DAYS));
            while (stream.next()) {
                // data received
                System.out.println(stream.getRoot().contentToTSVString());
            }
            long end = System.currentTimeMillis(); // time end
            System.out.println("Analyze response time: " + ((double) (end - start) / 1000) + " secs");
        }
    }

    public void executeParallelRequest(int numberOfParallelRequests){
        for (int i = 0; i < numberOfParallelRequests; i++) {
            new Thread(
                    new SimulateClientNodeParallel()
            ).start();
        }
    }

    public static void main(String[] args) throws InterruptedException {
            new ArrowFlightClientParallel(10);
    }
}
