// raft-core: consensus state machine. JDK only. Test deps (JUnit 5, jqwik) come from the root build.

val verifyNoRuntimeDependencies by tasks.registering {
    group = "verification"
    description = "Fails if raft-core declares any dependency on a production configuration."
    doLast {
        val offending = listOf("api", "implementation", "compileOnly", "runtimeOnly")
            .flatMap { name -> configurations.getByName(name).allDependencies.map { "$name: $it" } }
        check(offending.isEmpty()) {
            "raft-core must depend only on the JDK, found: $offending"
        }
    }
}

tasks.named("check") { dependsOn(verifyNoRuntimeDependencies) }
