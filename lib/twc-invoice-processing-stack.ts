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

    const detectInvoiceLambda = new lambda.Function(this, 'detectInvoiceLmabda', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lambda/detectInvoiceLambda')
    });

    // Create Step Functions State Machine
    const execUpdateAccountAssignmentLambda = new stepfunctions_tasks.LambdaInvoke(this, 'Update Account Assignment Details', {
      lambdaFunction: updateAccountAssignmentLambda
    });

    const execDetectInvoiceLambda = new stepfunctions_tasks.LambdaInvoke(this, 'Detect Invoice in Email', {
      lambdaFunction: detectInvoiceLambda
    });

    const definition = new stepfunctions.Choice(this, 'Account Assignment?')
      .when(stepfunctions.Condition.stringEquals('$.subjectContainsAccountAssignment', 'true'), execUpdateAccountAssignmentLambda)
      .otherwise(execDetectInvoiceLambda);

    const stateMachine = new stepfunctions.StateMachine(this, 'EmailProcessingSatetMachine', {
      definition,
      timeout: cdk.Duration.minutes(5),
    });

    // Grant processIncomingEmail Lambda permission to start the Step Function Execution and read from S3 bucket
    stateMachine.grantStartExecution(processIncomingEmailLambda);
    incomingEmailBucket.grantRead(processIncomingEmailLambda);

    // Set environment variables for the processIncomingEmail Lambda function
    processIncomingEmailLambda.addEnvironment('STATE_MACHINE_ARN', stateMachine.stateMachineArn);
    processIncomingEmailLambda.addEnvironment('BUCKET_NAME', incomingEmailBucket.bucketName);

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
