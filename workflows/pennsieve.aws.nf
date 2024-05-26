/*
 * Workflow Manager
 */
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
        python3.9 /service/taskRunner/init.py $ENVIRONMENT $x $y ${params.integrationID}
        """
    else
        """
        echo "running init\n"
        echo "Running int: using input $x, and input $y"
        """
}

process PreProcessor {
    debug true
    
    input:
        val x
        val y
        val z
    output:
        stdout

    script:
    if ("$ENVIRONMENT" != 'LOCAL')
        """
        python3.9 /service/taskRunner/pre_processor.py ${params.integrationID} ${params.apiKey} ${params.apiSecret}
        """
    else
        """
        echo "running pre-processor\n"
        echo "Running pre-processor: using input $x, and output $y"
        """
}

process Pipeline {
    debug true
    
    input:
        val pre_output
        val inputDir
        val outputDir
    output: stdout

    script:
    if ("$ENVIRONMENT" != 'LOCAL')
        """
        python3.9 /service/taskRunner/main.py $inputDir $outputDir
        """
    else
        """
        echo "running pipeline\n"
        echo "pre-output is: $pre_output"
        echo "inputDir is: $inputDir"
        echo "outputDir is: $outputDir"
        """
}

process PostProcessor {
    debug true

    input:
        val pipeline_output
        val outputDir
    output: stdout

    script:
        if ("$ENVIRONMENT" != 'LOCAL')
        """
        python3.9 /service/taskRunner/post_processor.py ${params.integrationID} ${params.apiKey} ${params.apiSecret}
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
    pre_ch = PreProcessor(input_ch, output_ch, init_ch)
    pipeline_ch = Pipeline(pre_ch, input_ch, output_ch)
    PostProcessor(pipeline_ch, output_ch)
}

workflow.onComplete {
    log.info ( workflow.success ? "\nDone!" : "Oops .. something went wrong" )
}