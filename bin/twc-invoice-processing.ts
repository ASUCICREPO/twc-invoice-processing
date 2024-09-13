#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { TwcInvoiceProcessingStack } from '../lib/twc-invoice-processing-stack';

const app = new cdk.App();

const domain = app.node.tryGetContext('domain');

if (!domain) {
  throw new Error('Domian must be provided. Use -c domain=<your-domain> when deploying.');
}

new TwcInvoiceProcessingStack(app, 'TwcInvoiceProcessingStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION
  },
  domain: domain
});