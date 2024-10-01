import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as ses from 'aws-cdk-lib/aws-ses';
import * as sesActions from 'aws-cdk-lib/aws-ses-actions';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as stepfunctions from 'aws-cdk-lib/aws-stepfunctions';
import * as stepfunctions_tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { Construct } from 'constructs';

interface TwcInvoiceProcessingStackProps extends cdk.StackProps {
  domain: string;
}

export class TwcInvoiceProcessingStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: TwcInvoiceProcessingStackProps) {
    super(scope, id, props);

    // Create the S3 bucket to store the incoming emails
    const incomingEmailBucket = new s3.Bucket(this, 'twc-incoming-email-bucket', {
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
      code: lambda.Code.fromAsset('lambda/processPDFAttachment')
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

    const savePdfToS3Lambda = new lambda.Function(this, 'savePdfToS3', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lambda/savePdfToS3')
    });

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

    const savePdfToS3Task = new stepfunctions_tasks.LambdaInvoke(this, 'Save PDF to S3', {
      lambdaFunction: savePdfToS3Lambda,
      outputPath: '$.Payload'
    });

    // Create Step Functions State Machine
    const definition = new stepfunctions.Choice(this, 'Check Subject')
      .when(stepfunctions.Condition.booleanEquals('$.subjectContainsAccountAssignment', true), updateAccountAssignmentTask)
      .otherwise(
        detectInvoiceTask
          .next(new stepfunctions.Choice(this, 'Process Attachment')
            .when(stepfunctions.Condition.stringEquals('$.attachmentType', 'excel'), 
              processExcelAttachmentTask.next(savePdfToS3Task))
            .when(stepfunctions.Condition.stringEquals('$.attachmentType', 'doc'),
              processDocAttachmentTask.next(savePdfToS3Task))
            .when(stepfunctions.Condition.stringEquals('$.attachmentType', 'text'),
              processEmailBodyTask.next(savePdfToS3Task))
            .when(stepfunctions.Condition.stringEquals('$.attachmentType', 'pdf'),
              processPDFAttachmentTask.next(savePdfToS3Task))
        )
      );

    const stateMachine = new stepfunctions.StateMachine(this, 'EmailProcessingSatetMachine', {
      definition,
      timeout: cdk.Duration.minutes(5),
    });

    // Grant permissions
    stateMachine.grantStartExecution(processIncomingEmailLambda);
    incomingEmailBucket.grantRead(processIncomingEmailLambda);
    incomingEmailBucket.grantReadWrite(updateAccountAssignmentLambda);
    incomingEmailBucket.grantReadWrite(detectInvoiceLambda);
    incomingEmailBucket.grantReadWrite(processPDFAttachmentLambda);
    incomingEmailBucket.grantReadWrite(processExcelAttachmentLambda);
    incomingEmailBucket.grantReadWrite(processDocAttachmentLambda);
    incomingEmailBucket.grantReadWrite(processEmailBodyLambda);
    incomingEmailBucket.grantReadWrite(savePdfToS3Lambda);

    // Set environment variables
    processIncomingEmailLambda.addEnvironment('STATE_MACHINE_ARN', stateMachine.stateMachineArn);
    processIncomingEmailLambda.addEnvironment('BUCKET_NAME', incomingEmailBucket.bucketName);
    updateAccountAssignmentLambda.addEnvironment('BUCKET_NAME', incomingEmailBucket.bucketName);
    detectInvoiceLambda.addEnvironment('BUCKET_NAME', incomingEmailBucket.bucketName);
    processPDFAttachmentLambda.addEnvironment('BUCKET_NAME', incomingEmailBucket.bucketName);
    processExcelAttachmentLambda.addEnvironment('BUCKET_NAME', incomingEmailBucket.bucketName);
    processDocAttachmentLambda.addEnvironment('BUCKET_NAME', incomingEmailBucket.bucketName);
    processEmailBodyLambda.addEnvironment('BUCKET_NAME', incomingEmailBucket.bucketName);
    savePdfToS3Lambda.addEnvironment('BUCKET_NAME', incomingEmailBucket.bucketName);

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
        new sesActions.S3({ bucket: incomingEmailBucket }),
        new sesActions.Lambda({ function: processIncomingEmailLambda })
      ]
    });

  }
}
