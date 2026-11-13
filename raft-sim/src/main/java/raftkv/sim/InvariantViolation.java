package raftkv.sim;

/** A safety invariant or the liveness bound was broken. */
public final class InvariantViolation extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public enum Kind {
        ELECTION_SAFETY, LEADER_APPEND_ONLY, LOG_MATCHING, LEADER_COMPLETENESS, STATE_MACHINE_SAFETY,
        LIVENESS, INTERNAL_ERROR
    }

    private final Kind kind;

    public InvariantViolation(Kind kind, String detail) {
        super(kind + ": " + detail);
        this.kind = kind;
    }

    public InvariantViolation(Kind kind, String detail, Throwable cause) {
        super(kind + ": " + detail, cause);
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }
}
