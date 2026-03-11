/*
 * Cross-Account ECR Pull Test
 *
 * Smoke test: registers a temporary ECS task definition pointing at
 * Account A's private ECR, runs it, and verifies the image pull succeeds.
 */

nextflow.enable.dsl=2

params.sourceUrl = ''
params.sourceVersion = ''
params.sessionToken = System.getenv('SESSION_TOKEN') ?: ''

log.info """\
    CROSS-ACCOUNT ECR PULL TEST
    ===================================
    sourceUrl: ${params.sourceUrl ?: 'MISSING'}
    sourceVersion: ${params.sourceVersion ?: 'MISSING'}
    SESSION_TOKEN: ${params.sessionToken ? 'provided (' + params.sessionToken.size() + ' chars)' : 'MISSING'}
    """.stripIndent(true)

process TestEcrPull {
    debug true

    output:
        stdout

    shell:
    '''
    export SESSION_TOKEN='!{params.sessionToken}'
    export SOURCE_URL='!{params.sourceUrl}'
    export SOURCE_VERSION='!{params.sourceVersion}'
    python3.9 /service/taskRunner/test_ecr_pull.py
    '''
}

workflow {
    TestEcrPull()
}

workflow.onComplete {
    log.info ( workflow.success ? "\nPASS: ECR cross-account pull test succeeded!" : "\nFAIL: ${workflow.errorMessage}" )
}
