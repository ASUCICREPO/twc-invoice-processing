import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as ses from 'aws-cdk-lib/aws-ses';
import * as sesActions from 'aws-cdk-lib/aws-ses-actions';
import { Construct } from 'constructs';
// import * as sqs from 'aws-cdk-lib/aws-sqs';

interface TwcInvoiceProcessingStackProps extends cdk.StackProps {
  domain: string;
}

export class TwcInvoiceProcessingStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: TwcInvoiceProcessingStackProps) {
    super(scope, id, props);

    // Create the S3 bucket to store the incoming emails
    const incomingEmailBucket = new s3.Bucket(this, 'twc-incoming-email-bucket', {
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });

    // Create SES Rule Set
    const sesRuleSet = new ses.ReceiptRuleSet(this, 'twc-email-rule-set', {
      receiptRuleSetName: 'twc-email-processing-rule-set',
    });

    // Create SES Receipt Rule
    const sesRule = sesRuleSet.addRule('twc-process-incoming-email', {
      recipients: [props.domain],
      scanEnabled: true,
      tlsPolicy: ses.TlsPolicy.REQUIRE
    });

    // Add actions to the rule
    sesRule.addAction(new sesActions.S3({
      bucket: incomingEmailBucket,
      objectKeyPrefix: 'incoming/'
    }));
  }
}
