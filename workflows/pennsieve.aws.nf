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
    
    input:
        val x
        val y
    output:
        stdout

    script:
    if ("$ENVIRONMENT" != 'LOCAL')
        """
        python3.9 /service/taskRunner/init.py ${params.integrationID} ${params.apiKey} ${params.apiSecret}
        """
    else
        """
        python3.9 /service/taskRunner/init_local.py
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
    if ("$ENVIRONMENT" != 'LOCAL')
        """
        python3.9 /service/taskRunner/multi_stage.py ${params.integrationID} ${params.apiKey} ${params.apiSecret} '$wf' $inputDir $outputDir
        """
    else
        """
        echo "running local multi-stage-processor\n"
        python3.9 /service/taskRunner/multi_stage_local.py '$wf'
        """
}

process CleanupWorkflow {
    debug true
    
    input:
        val multiStageOutput
        val inputDir
        val outputDir

    output:
        stdout

    script:
    if ("$ENVIRONMENT" != 'LOCAL')
        """
        python3.9 /service/taskRunner/cleanup.py '$multiStageOutput' $inputDir $outputDir
        """
    else
        """
        echo "running local cleanup\n"
        python3.9 /service/taskRunner/cleanup_local.py '$multiStageOutput' $inputDir $outputDir
        """
}

workflow {
    input_ch = Channel.of(params.inputDir)
    output_ch = Channel.of(params.outputDir)
    key_ch = Channel.of(params.apiKey)
    secret_ch = Channel.of(params.apiSecret)

    wf_ch = InitWorkflow(key_ch, secret_ch)
    ms_ch = MultiStageWorkflow(input_ch, output_ch, wf_ch)
    CleanupWorkflow(ms_ch, input_ch, output_ch)
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