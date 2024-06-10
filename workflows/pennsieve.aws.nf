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

process PreProcessor {
    debug true
    
    input:
        val x
        val y
        val w
    output:
        stdout

    script:
    if ("$ENVIRONMENT" != 'LOCAL')
        """
        python3.9 /service/taskRunner/pre_processor.py ${params.integrationID} ${params.apiKey} ${params.apiSecret} '$w'
        """
    else
        """
        echo "running local pre-processor\n"
        python3.9 /service/taskRunner/pre_processor_local.py '$w'
        """
}

process Pipeline {
    debug true
    
    input:
        val pre_output
        val inputDir
        val outputDir
        val w
    output: stdout

    script:
    if ("$ENVIRONMENT" != 'LOCAL')
        """
        python3.9 /service/taskRunner/main.py $inputDir $outputDir '$w'
        """
    else
        """
        echo "running pipeline\n"
        echo "pre-output is: $pre_output"
        echo "inputDir is: $inputDir"
        echo "outputDir is: $outputDir"
        echo '$w'
        """
}

process PostProcessor {
    debug true

    input:
        val pipeline_output
        val outputDir
        val w
    output: stdout

    script:
        if ("$ENVIRONMENT" != 'LOCAL')
        """
        python3.9 /service/taskRunner/post_processor.py ${params.integrationID} ${params.apiKey} ${params.apiSecret} '$w'
        """
    else
        """
        echo "running post-processor\n"
        echo "pipeline_output is: $pipeline_output"
        echo "outputDir is: $outputDir"
        """
}

workflow {
    input_ch = Channel.of(params.inputDir)
    output_ch = Channel.of(params.outputDir)
    key_ch = Channel.of(params.apiKey)
    secret_ch = Channel.of(params.apiSecret)

    init_ch = InitWorkflow(key_ch, secret_ch)
    MultiStageWorkflow(input_ch, output_ch, init_ch)
}

workflow.onComplete {
    log.info ( workflow.success ? "\nDone!" : "Oops .. something went wrong" )
}