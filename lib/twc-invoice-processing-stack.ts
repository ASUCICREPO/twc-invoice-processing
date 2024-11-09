import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as ses from 'aws-cdk-lib/aws-ses';
import * as sesActions from 'aws-cdk-lib/aws-ses-actions';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as stepfunctions from 'aws-cdk-lib/aws-stepfunctions';
import * as stepfunctions_tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';

interface TwcInvoiceProcessingStackProps extends cdk.StackProps {
  domain: string;
  senderEmail: string;
  recipientEmails: string[];
}

export class TwcInvoiceProcessingStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: TwcInvoiceProcessingStackProps) {
    super(scope, id, props);

    // Create the S3 buckets
    const incomingEmailBucket = new s3.Bucket(this, 'twc-incoming-email-bucket', {
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      versioned: true,
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [
        {
          // Move non-current versions to infrequent access after 30 days
          noncurrentVersionTransitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(30)
            }
          ],
          // Delete non-current versions after 60 days
          noncurrentVersionExpiration: cdk.Duration.days(60),
          // Delete objects after 90 days
          expiration: cdk.Duration.days(90)
        }
      ]
    });

    const artefactBucket = new s3.Bucket(this, 'twc-artefact-bucket', {
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      versioned: true,
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [
        {
          // Rule for textract_results directory
          prefix: 'textract_results/',
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(30)  // Move to IA after a month
            }
          ],
          noncurrentVersionExpiration: cdk.Duration.days(60),  // Keep versions for 60 days
          expiration: cdk.Duration.days(90)  // Delete after 90 days
        },
        {
          // Rule for invoices directory
          prefix: 'invoices/',
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(30)  // Move to IA after a month
            }
          ],
          noncurrentVersionExpiration: cdk.Duration.days(60),  // Keep versions for 60 days
          expiration: cdk.Duration.days(90)  // Delete after 90 days
        }
      ]
    });
  
    const resultBucket = new s3.Bucket(this, 'twc-result-bucket', {
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      versioned: true,
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [
        {
          // Since files are emailed daily, we can be more aggressive with the cleanup
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(30)  // Move to IA after a month
            }
          ],
          noncurrentVersionTransitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(30)  // Move non-current versions to IA after a month
            }
          ],
          noncurrentVersionExpiration: cdk.Duration.days(60),  // Delete non-current versions after 60 days
          expiration: cdk.Duration.days(90)  // Keep files for 90 days total
        }
      ]
    });

    // Create Lambda functions
    const processIncomingEmailLambda = new lambda.Function(this, 'processIncomingEmail', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lambda/processIncomingEmail'),
      environment: {
        BUCKET_NAME: incomingEmailBucket.bucketName
      },
    });
    incomingEmailBucket.grantRead(processIncomingEmailLambda);

    const updateAccountAssignmentLambda = new lambda.Function(this, 'updateAccountAssignment', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lambda/updateAccountAssignment'),
      environment: {
        EMAIL_BUCKET_NAME: incomingEmailBucket.bucketName,
        ARTEFACT_BUCKET_NAME: artefactBucket.bucketName
      },
    });
    incomingEmailBucket.grantRead(updateAccountAssignmentLambda);
    artefactBucket.grantWrite(updateAccountAssignmentLambda);

    const detectInvoiceLambda = new lambda.Function(this, 'detectInvoice', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lambda/detectInvoice'),
      environment: {
        EMAIL_BUCKET_NAME: incomingEmailBucket.bucketName,
      }
    });
    incomingEmailBucket.grantReadWrite(detectInvoiceLambda);

    const processPDFAttachmentLambda = new lambda.Function(this, 'processPDFAttachment', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lambda/processPDFAttachment'),
      environment: {
        EMAIL_BUCKET_NAME: incomingEmailBucket.bucketName,
        ARTEFACT_BUCKET_NAME: artefactBucket.bucketName
      },
      timeout: cdk.Duration.seconds(60),
      memorySize: 256
    });
    incomingEmailBucket.grantRead(processPDFAttachmentLambda);
    artefactBucket.grantWrite(processPDFAttachmentLambda);

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
      environment: {
        EMAIL_BUCKET_NAME: incomingEmailBucket.bucketName,
        ARTEFACT_BUCKET_NAME: artefactBucket.bucketName
      },
      timeout: cdk.Duration.seconds(60),
      memorySize: 256
    });
    incomingEmailBucket.grantRead(processExcelAttachmentLambda);
    artefactBucket.grantWrite(processExcelAttachmentLambda);

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
      environment: {
        EMAIL_BUCKET_NAME: incomingEmailBucket.bucketName,
        ARTEFACT_BUCKET_NAME: artefactBucket.bucketName
      },
      timeout: cdk.Duration.seconds(60),
      memorySize: 256
    });
    incomingEmailBucket.grantRead(processDocAttachmentLambda);
    artefactBucket.grantWrite(processDocAttachmentLambda);

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
        environment: {
          EMAIL_BUCKET_NAME: incomingEmailBucket.bucketName,
          ARTEFACT_BUCKET_NAME: artefactBucket.bucketName
        }, 
        timeout: cdk.Duration.seconds(60),
        memorySize: 256
    });
    incomingEmailBucket.grantRead(processEmailBodyLambda);
    artefactBucket.grantWrite(processEmailBodyLambda);

    const startTextractJobLambda = new lambda.Function(this, 'startTextractJob', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'startTextractJob.handler',
      code: lambda.Code.fromAsset('lambda/textractAnalysis'),
      environment: {
        ARTEFACT_BUCKET_NAME: artefactBucket.bucketName,
      },
    });
    startTextractJobLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['textract:StartExpenseAnalysis'],
      resources: ['*']
    }));
    artefactBucket.grantRead(startTextractJobLambda);

    const getTextractResultsLambda = new lambda.Function(this, 'getTextractResults', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'getTextractResults.handler',
      code: lambda.Code.fromAsset('lambda/textractAnalysis'),
      environment: {
        ARTEFACT_BUCKET_NAME: artefactBucket.bucketName,
      },
    });
    artefactBucket.grantWrite(getTextractResultsLambda);
    getTextractResultsLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['textract:GetExpenseAnalysis'],
      resources: ['*']
    }));
    
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
        INPUT_BUCKET_NAME: incomingEmailBucket.bucketName,
        ARTEFACT_BUCKET_NAME: artefactBucket.bucketName,
        RESULT_BUCKET_NAME: resultBucket.bucketName, 
        TIMEZONE: 'America/Chicago'  // TODO: make env var
      },
    });
    incomingEmailBucket.grantRead(processTextractResultsLambda);
    artefactBucket.grantRead(processTextractResultsLambda)
    resultBucket.grantReadWrite(processTextractResultsLambda);
    processTextractResultsLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['bedrock:InvokeModel'],
      resources: ['*']
    }));

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
    processIncomingEmailLambda.addEnvironment('STATE_MACHINE_ARN', stateMachine.stateMachineArn);
    stateMachine.grantStartExecution(processIncomingEmailLambda);

    const sendDailyEmailLambda = new lambda.Function(this, 'sendDailyEmail', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lambda/sendDailyEmail'),
      environment: {
        RESULT_BUCKET_NAME: resultBucket.bucketName,
        SENDER_EMAIL: props.senderEmail,
        RECIPIENT_EMAILS: props.recipientEmails.join(',')
      },
      timeout: cdk.Duration.minutes(5)
    });
    
    // Grant permissions
    resultBucket.grantRead(sendDailyEmailLambda);
    sendDailyEmailLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['ses:SendRawEmail'],
      resources: ['*']
    }));
    
    new events.Rule(this, 'weekdayReportSchedule', {
      schedule: events.Schedule.cron({
        minute: '0',
        hour: '23',
        weekDay: 'MON-FRI',
        month: '*',
        year: '*'
      }),
      targets: [new targets.LambdaFunction(sendDailyEmailLambda)]
    });
    
    // Verify sender email in SES
    new ses.EmailIdentity(this, 'SenderEmailIdentity', {
      identity: ses.Identity.email(props.senderEmail)
    });
    
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
