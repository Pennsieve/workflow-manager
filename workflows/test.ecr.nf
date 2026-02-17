/*
 * Cross-Account ECR Pull Test
 *
 * Smoke test: registers a temporary ECS task definition pointing at
 * Account A's private ECR, runs it, and verifies the image pull succeeds.
 */

nextflow.enable.dsl=2

log.info """\
    CROSS-ACCOUNT ECR PULL TEST
    ===================================
    """.stripIndent(true)

process TestEcrPull {
    debug true

    output:
        stdout

    script:
    """
    python3.9 /service/taskRunner/test_ecr_pull.py
    """
}

workflow {
    TestEcrPull()
}

workflow.onComplete {
    log.info ( workflow.success ? "\nPASS: ECR cross-account pull test succeeded!" : "\nFAIL: ${workflow.errorMessage}" )
}
