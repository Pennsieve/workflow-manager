/*
 * Workflow Manager
 */

 nextflow.enable.dsl=2

log.info """\
    WORKFLOW MANAGER
    ===================================
    """
    .stripIndent(true)

params.inputDir = "$BASE_DIR/input/${params.integrationID}"
params.outputDir = "$BASE_DIR/output/${params.integrationID}"

process InitWorkflow {
    debug true

    output:
        stdout

    script:
    """
    python3.9 /service/taskRunner/init.py ${params.integrationID} ${params.apiKey} ${params.apiSecret}
    """
}

process MultiStageWorkflow {
    debug true

    input:
        val inputDir
        val outputDir
        val wf
    output:
        stdout

    script:
    """
    python3.9 /service/taskRunner/multi_stage.py '${params.integrationID}' '${params.apiKey}' '${params.apiSecret}' '$wf' '$inputDir' '$outputDir' '${params.workspaceDir}' '${params.resourcesDir}'
    """
}

process FinaliseWorkflow {
    debug true
    
    input:
        val wf
    output:
        stdout

    script:
    """
    python3.9 /service/taskRunner/finalise.py ${params.integrationID} ${params.apiKey} ${params.apiSecret}
    """
}

workflow {
    input_ch = Channel.of(params.inputDir)
    output_ch = Channel.of(params.outputDir)

    init_ch = InitWorkflow()
    wf_ch = MultiStageWorkflow(input_ch, output_ch, init_ch)
    FinaliseWorkflow(wf_ch)
}

workflow.onComplete {
    log.info ( workflow.success ? "\nDone!" : "Oops .. something went wrong: ${workflow.errorMessage}" )

    println "--------------------------"
    println "Pipeline execution summary"
    println "--------------------------"
    println "Started at  : ${workflow.start}"
    println "Completed at: ${workflow.complete}"
    println "Duration    : ${workflow.duration}"
    println "Success     : ${workflow.success}"
    println "WorkDir     : ${workflow.workDir}"
}
