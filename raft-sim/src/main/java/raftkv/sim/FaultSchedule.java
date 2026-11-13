package raftkv.sim;

/** Per-event fault probabilities. */
public record FaultSchedule(double drop, double duplicate, int maxDelayTicks, double partitionChange,
                            double crash, double restart, double propose) {

    public FaultSchedule {
        for (double p : new double[] {drop, duplicate, partitionChange, crash, restart, propose}) {
            if (p < 0 || p > 1) {
                throw new IllegalArgumentException("probability out of range: " + p);
            }
        }
        if (maxDelayTicks < 1) {
            throw new IllegalArgumentException("maxDelayTicks must be >= 1");
        }
    }

    public static FaultSchedule none() {
        return new FaultSchedule(0, 0, 2, 0, 0, 0, 0.02);
    }

    public static FaultSchedule chaos() {
        return new FaultSchedule(0.1, 0.05, 8, 0.004, 0.002, 0.01, 0.03);
    }
}
