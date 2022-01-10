package usecase.flight;

import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.VectorSchemaRoot;

public class ArrowFlightServerProducer extends NoOpFlightProducer {
    private int simulateHorizontalScalability;

    public ArrowFlightServerProducer(int simulateHorizontalScalability) {
        this.simulateHorizontalScalability = simulateHorizontalScalability;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        // consider: we have this vector schema root needed to be transfer to the different clients
        VectorSchemaRoot vectorSchemaRoot = io.Util.createVectorSchemaRoot();

        // simulate horizontal scalability
        for (int i = 0; i < simulateHorizontalScalability; i++) {
            if (i == 0) {
                /**
                 * Start sending data, using the schema of the given {@link VectorSchemaRoot}.
                 *
                 * <p>This method must be called before all others, except {@link #putMetadata(ArrowBuf)}.
                 */
                listener.start(vectorSchemaRoot);
            }

            /**
             * Send the current contents of the associated {@link VectorSchemaRoot}.
             *
             * <p>This will not necessarily block until the message is actually sent; it may buffer messages
             * in memory. Use {@link #isReady()} to check if there is backpressure and avoid excessive buffering.
             */
            listener.putNext();
        }
        /**
         * Indicate that transmission is finished.
         */
        listener.completed();
    }
}
