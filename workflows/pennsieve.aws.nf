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
        echo 'cbc01408-e39b-4238-a127-0ed6d36f2e6a,37fd4524-1efc-42a1-a81a-0f82f0467c23,1845c2c6-b60c-49a6-893f-1761033f5987'
        """
    else
        """
        echo 'preprocessor123,processor123,postprocessor123'
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
    workflow = w.split(',') as List
    app_uuid = workflow[0]
    if ("$ENVIRONMENT" != 'LOCAL')
        """
        python3.9 /service/taskRunner/pre_processor.py ${params.integrationID} ${params.apiKey} ${params.apiSecret} ${app_uuid}
        """
    else
        """
        echo "running pre-processor\n"
        echo "Running pre-processor: using input $x, and output $y, and ${app_uuid}"
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
    workflow = w.split(',') as List
    app_uuid = workflow[1]
    if ("$ENVIRONMENT" != 'LOCAL')
        """
        python3.9 /service/taskRunner/main.py $inputDir $outputDir ${app_uuid} ${params.apiKey} ${params.apiSecret}
        """
    else
        """
        echo "running pipeline\n"
        echo "pre-output is: $pre_output"
        echo "inputDir is: $inputDir"
        echo "outputDir is: $outputDir"
        echo "processor is: $app_uuid"
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
        workflow = w.split(',') as List
        app_uuid = workflow[2]
        if ("$ENVIRONMENT" != 'LOCAL')
        """
        python3.9 /service/taskRunner/post_processor.py ${params.integrationID} ${params.apiKey} ${params.apiSecret} ${app_uuid}
        """
    else
        """
        echo "running post-processor\n"
        echo "pipeline_output is: $pipeline_output"
        echo "outputDir is: $outputDir"
        echo "processor is: $app_uuid"
        """
}

workflow {
    input_ch = Channel.of(params.inputDir)
    output_ch = Channel.of(params.outputDir)
    key_ch = Channel.of(params.apiKey)
    secret_ch = Channel.of(params.apiSecret)

    init_ch = InitWorkflow(key_ch, secret_ch)
    pre_ch = PreProcessor(input_ch, output_ch, init_ch)
    pipeline_ch = Pipeline(pre_ch, input_ch, output_ch, init_ch)
    PostProcessor(pipeline_ch, output_ch, init_ch)
}

workflow.onComplete {
    log.info ( workflow.success ? "\nDone!" : "Oops .. something went wrong" )
}