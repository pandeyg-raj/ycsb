package site.ycsb;

import org.apache.commons.math3.distribution.ExponentialDistribution;

public class PoissonArrivalGenerator {

    private final ExponentialDistribution expDist;
    private long nextArrivalTimeNanos;

    public PoissonArrivalGenerator(double lambda) {
        // lambda is in requests per second, so mean inter-arrival time = 1 / lambda
        this.expDist = new ExponentialDistribution(1.0 / lambda);
        this.nextArrivalTimeNanos = System.nanoTime();
    }

    /**
     * Sleeps until the next arrival time according to the Poisson process.
     */
    public void sleepUntilNextArrival() {
        double gapSeconds = expDist.sample(); // time until next request in seconds
        nextArrivalTimeNanos += (long) (gapSeconds * 1e9);

        long now = System.nanoTime();
        long sleepTime = nextArrivalTimeNanos - now;

        if (sleepTime > 0) {
            try {
                Thread.sleep(sleepTime / 1_000_000, (int) (sleepTime % 1_000_000));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            // If we're behind schedule, just continue immediately
            nextArrivalTimeNanos = now;
        }
    }
}
