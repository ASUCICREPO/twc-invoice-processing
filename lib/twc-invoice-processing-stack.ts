import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as ses from 'aws-cdk-lib/aws-ses';
import * as sesActions from 'aws-cdk-lib/aws-ses-actions';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as stepfunctions from 'aws-cdk-lib/aws-stepfunctions';
import * as stepfunctions_tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

interface TwcInvoiceProcessingStackProps extends cdk.StackProps {
  domain: string;
}

export class TwcInvoiceProcessingStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: TwcInvoiceProcessingStackProps) {
    super(scope, id, props);

    // Create the S3 buckets
    const inputBucket = new s3.Bucket(this, 'twc-input-bucket', {
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      versioned: true,
      encryption: s3.BucketEncryption.S3_MANAGED
    });

    const outputBucket = new s3.Bucket(this, 'twc-output-bucket', {
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      versioned: true,
      encryption: s3.BucketEncryption.S3_MANAGED
    });

    // Create Lambda functions
    const processIncomingEmailLambda = new lambda.Function(this, 'processIncomingEmail', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lambda/processIncomingEmail')
    });

    const updateAccountAssignmentLambda = new lambda.Function(this, 'updateAccountAssignment', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lambda/updateAccountAssignment')
    });

    const detectInvoiceLambda = new lambda.Function(this, 'detectInvoice', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lambda/detectInvoice'),
    });

    const processPDFAttachmentLambda = new lambda.Function(this, 'processPDFAttachment', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lambda/processPDFAttachment'),
      timeout: cdk.Duration.seconds(60),
      memorySize: 256
    });

    const processExcelAttachmentLambda = new lambda.Function(this, 'processExcelAttachment', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lambda/processExcelAttachment',
        {
          bundling: {
            image: lambda.Runtime.PYTHON_3_12.bundlingImage,
            command: [
              'bash', '-c',
              'pip install -r requirements.txt -t /asset-output && cp index.py /asset-output'
            ],
          },
        }
      ),    
      timeout: cdk.Duration.seconds(60),
      memorySize: 256
    });

    const processDocAttachmentLambda = new lambda.Function(this, 'processDocAttachment', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lambda/processDocAttachment',
        {
          bundling: {
            image: lambda.Runtime.PYTHON_3_12.bundlingImage,
            command: [
              'bash', '-c',
              'pip install -r requirements.txt -t /asset-output && cp index.py /asset-output'
            ],
          },
        }
      ),    
      timeout: cdk.Duration.seconds(60),
      memorySize: 256
    });

    const processEmailBodyLambda = new lambda.Function(this, 'processEmailBody', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lambda/processEmailBody',
        {
          bundling: {
            image: lambda.Runtime.PYTHON_3_12.bundlingImage,
            command: [
              'bash', '-c',
              'pip install -r requirements.txt -t /asset-output && cp index.py /asset-output'
            ],
          },
        }),    
        timeout: cdk.Duration.seconds(60),
        memorySize: 256
    });

    const startTextractJobLambda = new lambda.Function(this, 'startTextractJob', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'startTextractJob.handler',
      code: lambda.Code.fromAsset('lambda/textractAnalysis'),
      environment: {
        INPUT_BUCKET_NAME: inputBucket.bucketName,
      },
    });

    const getTextractResultsLambda = new lambda.Function(this, 'getTextractResults', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'getTextractResults.handler',
      code: lambda.Code.fromAsset('lambda/textractAnalysis'),
      environment: {
        OUTPUT_BUCKET_NAME: outputBucket.bucketName,
      },
    });
    
    const processTextractResultsLambda = new lambda.Function(this, 'processTextractResults', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lambda/processTextractResults',
        {
          bundling: {
            image: lambda.Runtime.PYTHON_3_12.bundlingImage,
            command: [
              'bash', '-c',
              'pip install -r requirements.txt -t /asset-output && cp index.py /asset-output'
            ],
          },
        }),
      timeout: cdk.Duration.seconds(300),
      memorySize: 1024,
      environment: {
        INPUT_BUCKET_NAME: inputBucket.bucketName,
        OUTPUT_BUCKET_NAME: outputBucket.bucketName,
        TIMEZONE: 'America/Chicago'  // TODO: make env var
      },
    });

    // Grant Textract permissions to the Lambda
    startTextractJobLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['textract:StartExpenseAnalysis'],
      resources: ['*']
    }));
    getTextractResultsLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['textract:GetExpenseAnalysis'],
      resources: ['*']
    }));

    // Grant S3 read/write permissions to the Lambda
    inputBucket.grantRead(startTextractJobLambda);
    outputBucket.grantWrite(getTextractResultsLambda);
    inputBucket.grantRead(processTextractResultsLambda);
    outputBucket.grantReadWrite(processTextractResultsLambda);

    // Create Step Functions tasks
    const updateAccountAssignmentTask = new stepfunctions_tasks.LambdaInvoke(this,'Update Account Assignment', {
      lambdaFunction: updateAccountAssignmentLambda,
      outputPath: '$.Payload'
    });

    const detectInvoiceTask = new stepfunctions_tasks.LambdaInvoke(this, 'Detect Invoice', {
      lambdaFunction: detectInvoiceLambda,
      outputPath: '$.Payload'
    });

    const processPDFAttachmentTask = new stepfunctions_tasks.LambdaInvoke(this, 'Process PDF Attachment', {
      lambdaFunction: processPDFAttachmentLambda,
      outputPath: '$.Payload'
    });

    const processExcelAttachmentTask = new stepfunctions_tasks.LambdaInvoke(this, 'Process Excel Attachment', {
      lambdaFunction: processExcelAttachmentLambda,
      outputPath: '$.Payload'
    });

    const processDocAttachmentTask = new stepfunctions_tasks.LambdaInvoke(this, 'Process Doc Attachment', {
      lambdaFunction: processDocAttachmentLambda,
      outputPath: '$.Payload'
    });

    const processEmailBodyTask = new stepfunctions_tasks.LambdaInvoke(this, 'Process Email Body', {
      lambdaFunction: processEmailBodyLambda,
      outputPath: '$.Payload'
    });

    const startTextractJobTask = new stepfunctions_tasks.LambdaInvoke(this, 'Start Textract Jobs', {
      lambdaFunction: startTextractJobLambda,
      outputPath: '$.Payload',
    });
    
    const getTextractResultsTask = new stepfunctions_tasks.LambdaInvoke(this, 'Get Textract Results', {
      lambdaFunction: getTextractResultsLambda,
      outputPath: '$.Payload',
    });
    
    const processTextractResultsTask = new stepfunctions_tasks.LambdaInvoke(this, 'Process Textract Results', {
      lambdaFunction: processTextractResultsLambda,
      outputPath: '$.Payload',
    });

    // Create Step function States
    const wait30Seconds = new stepfunctions.Wait(this, 'Wait 30 Seconds', {
      time: stepfunctions.WaitTime.duration(cdk.Duration.seconds(30)),
    });

    const asyncTextractProcessing = 
      startTextractJobTask
        .next(wait30Seconds)
        .next(getTextractResultsTask)
        .next(
          new stepfunctions.Choice(this, 'Job Complete?')
            .when(stepfunctions.Condition.stringEquals('$.jobStatus', 'SUCCEEDED'), 
              processTextractResultsTask)
            .when(stepfunctions.Condition.stringEquals('$.jobStatus', 'IN_PROGRESS'), 
              wait30Seconds)
            .otherwise(
              new stepfunctions.Fail(this, 'Textract Job Failed', {
                cause: 'Textract job failed or timed out',
                error: 'TextractJobError',
              })
            )
        );

    const processAttachment = new stepfunctions.Choice(this, 'Process Attachment')
      .when(stepfunctions.Condition.stringEquals('$.type', 'pdf'), 
        processPDFAttachmentTask)
      .when(stepfunctions.Condition.stringEquals('$.type', 'excel'),
        processExcelAttachmentTask)
      .when(stepfunctions.Condition.stringEquals('$.type', 'doc'),
        processDocAttachmentTask)
      .otherwise(processEmailBodyTask);

    const processAttachmentMap = new stepfunctions.Map(this, 'Process Attachments', {
      maxConcurrency: 5, // Adjust this value based on your requirements
      itemsPath: stepfunctions.JsonPath.stringAt('$.attachments'),
      parameters: {
        'type.$': '$$.Map.Item.Value.type',
        'filename.$': '$$.Map.Item.Value.filename',
        'messageId.$': '$.messageId',
        'bucketName.$': '$.bucketName'
      }
    });

    processAttachmentMap.itemProcessor(processAttachment);

    const definition = new stepfunctions.Choice(this, 'Check Subject')
      .when(stepfunctions.Condition.booleanEquals('$.subjectContainsAccountAssignment', true), updateAccountAssignmentTask)
      .otherwise(
        detectInvoiceTask
          .next(new stepfunctions.Choice(this, 'Check Attachments')
            .when(stepfunctions.Condition.isPresent('$.attachments[0]'),
              processAttachmentMap
                .next(asyncTextractProcessing)
            )
          )
      );

    const stateMachine = new stepfunctions.StateMachine(this, 'EmailProcessingSatetMachine', {
      definition,
      timeout: cdk.Duration.minutes(15),
    });

    // Grant permissions
    stateMachine.grantStartExecution(processIncomingEmailLambda);
    inputBucket.grantRead(processIncomingEmailLambda);
    inputBucket.grantReadWrite(updateAccountAssignmentLambda);
    inputBucket.grantReadWrite(detectInvoiceLambda);
    inputBucket.grantReadWrite(processPDFAttachmentLambda);
    inputBucket.grantReadWrite(processExcelAttachmentLambda);
    inputBucket.grantReadWrite(processDocAttachmentLambda);
    inputBucket.grantReadWrite(processEmailBodyLambda);

    // Set environment variables
    processIncomingEmailLambda.addEnvironment('STATE_MACHINE_ARN', stateMachine.stateMachineArn);
    processIncomingEmailLambda.addEnvironment('BUCKET_NAME', inputBucket.bucketName);
    updateAccountAssignmentLambda.addEnvironment('BUCKET_NAME', inputBucket.bucketName);
    detectInvoiceLambda.addEnvironment('BUCKET_NAME', inputBucket.bucketName);
    processPDFAttachmentLambda.addEnvironment('BUCKET_NAME', inputBucket.bucketName);
    processExcelAttachmentLambda.addEnvironment('BUCKET_NAME', inputBucket.bucketName);
    processDocAttachmentLambda.addEnvironment('BUCKET_NAME', inputBucket.bucketName);
    processEmailBodyLambda.addEnvironment('BUCKET_NAME', inputBucket.bucketName);

    // Create SES Rule Set
    const sesRuleSet = new ses.ReceiptRuleSet(this, 'twc-email-rule-set', {
      receiptRuleSetName: 'twc-email-processing-rule-set',
    });

    // Create SES Receipt Rule
    sesRuleSet.addRule('twc-process-incoming-email', {
      recipients: [props.domain],
      scanEnabled: true,
      tlsPolicy: ses.TlsPolicy.REQUIRE,
      actions: [
        new sesActions.S3({ bucket: inputBucket }),
        new sesActions.Lambda({ function: processIncomingEmailLambda })
      ]
    });

  }
}
