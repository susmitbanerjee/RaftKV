package raftkv.core;

/** Test-only fault injection for mutation testing. Production code must use {@link #NONE}. */
public enum Bug {
    NONE,
    /** Skips the election restriction. */
    SKIP_UP_TO_DATE_CHECK,
    /** Commits older-term entries by counting replicas. */
    COMMIT_OLDER_TERM_BY_COUNTING,
    /** Ignores votedFor, so a node can vote twice in one term. */
    TWO_VOTES_PER_TERM,
    /** Keeps conflicting entries instead of deleting them. */
    NEVER_TRUNCATE,
    /** Does not reset votedFor when the term advances. */
    KEEP_VOTE_ON_NEW_TERM,
    /** Applies entries as soon as they are appended, before they are committed. */
    APPLY_BEFORE_COMMIT,
    /** A leader truncates its own uncommitted tail before appending. */
    LEADER_OVERWRITES_OWN_LOG
}
