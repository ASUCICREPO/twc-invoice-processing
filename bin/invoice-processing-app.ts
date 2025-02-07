#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { InvoiceProcessingStack } from '../lib/invoice-processing-app-stack';

// Hardcoded AWS credentials
const awsConfig = {
  account: '<account-number>',  // e.g., '123456789012'
  region: '<region>',       // e.g., 'us-east-1'
  credentials: {
    accessKeyId: '<IAM-user-access-key>',
    secretAccessKey: '<IAM-user-secret-key>'
  }
};

const app = new cdk.App();

const domain = app.node.tryGetContext('domain');
const senderEmail = app.node.tryGetContext('senderEmail');
const recipientEmailsStr = app.node.tryGetContext('recipientEmails');

const recipientEmails = recipientEmailsStr ? recipientEmailsStr.split(',') : [];

if (!domain || !senderEmail || recipientEmails.length === 0) {
  throw new Error('Missing required context values. Please provide domain, senderEmail, and recipientEmails');
}

new InvoiceProcessingStack(app, 'InvoiceProcessingStack', {
  env: {
    account: awsConfig.account,
    region: awsConfig.region,
  },
  domain: domain,
  senderEmail: senderEmail,
  recipientEmails: recipientEmails
});

// Configure AWS SDK credentials
process.env.AWS_ACCESS_KEY_ID = awsConfig.credentials.accessKeyId;
process.env.AWS_SECRET_ACCESS_KEY = awsConfig.credentials.secretAccessKey;
process.env.AWS_REGION = awsConfig.region;