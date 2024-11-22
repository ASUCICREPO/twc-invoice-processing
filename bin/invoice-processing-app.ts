#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { InvoiceProcessingStack } from '../lib/invoice-processing-app-stack';

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
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION
  },
  domain: domain,
  senderEmail: senderEmail,
  recipientEmails:recipientEmails 
});
