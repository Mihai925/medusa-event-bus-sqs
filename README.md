# medusa-event-bus-sqs

![Medusa](https://user-images.githubusercontent.com/7554214/153162406-bf8fd16f-aa98-4604-b87b-e13ab4baf604.png)

## Overview

**UNOFFICIAL PLUGIN** - This is a community-created plugin for Medusa and is not officially supported by the Medusa team.

SQS Event Bus module for Medusa. When installed, the events system of Medusa is powered by AWS Simple Queue Service (SQS). This plugin provides a reliable, scalable event bus solution using Amazon's managed messaging service.

This plugin extends Medusa's event system to work with SQS, allowing for distributed event processing across multiple instances and improved reliability for event-driven architectures.

## Features

- **Reliable Message Delivery**: Leverages AWS SQS for at-least-once delivery guarantees
- **Event Grouping**: Support for grouping related events using `eventGroupId`
- **Scalable Architecture**: Handles high-volume event streams with SQS's auto-scaling
- **Flexible Polling**: Configurable polling intervals and batch sizes
- **AWS Authentication**: Support for multiple AWS authentication methods
- **Event Filtering**: Option to skip events without subscribers to reduce costs

## Prerequisites

Before using this plugin, you need:

1. An AWS account with appropriate permissions
2. An SQS queue created in your AWS account
3. AWS credentials configured (either via environment variables, IAM roles, or explicit credentials)

## Installation

Install the module:

```bash
npm install medusa-event-bus-sqs
```

or with yarn:

```bash
yarn add medusa-event-bus-sqs
```

## Configuration

Add the module to your `medusa-config.js`:

```javascript
module.exports = {
  // ...
  modules: [
   {
      [Modules.EVENT_BUS]: {
         resolve: "medusa-event-bus-sqs",
         options: {
         queueUrl: process.env.SQS_QUEUE_URL,
         region: process.env.SQS_REGION || "us-east-1",
         // Optional: Provide explicit credentials
         // useCredentials: true,
         // accessKeyId: "your-access-key-id",
         // secretAccessKey: "your-secret-access-key",
         // sessionToken: "your-session-token", // If using temporary credentials
         },
      },
   },
  ],
  // ...
}
```

### Configuration Options

| Option | Type | Description | Default | Required |
| --- | --- | --- | --- | --- |
| `queueUrl` | `string` | URL of the SQS queue to connect to | - | Yes |
| `region` | `string` | AWS region where the SQS queue is located | - | Yes |
| `useCredentials` | `boolean` | Whether to use explicit credentials provided below | `false` | No |
| `accessKeyId` | `string` | AWS access key ID | - | No |
| `secretAccessKey` | `string` | AWS secret access key | - | No |
| `sessionToken` | `string` | AWS session token (for temporary credentials) | - | No |
| `waitTimeSeconds` | `number` | Long polling wait time for SQS (1-20 seconds) | `20` | No |
| `pollingWaitTimeMs` | `number` | Delay between polling cycles in milliseconds | `0` | No |
| `maxNumberOfMessages` | `number` | Maximum messages to receive in one poll (1-10) | `10` | No |
| `skipUnsubscribedEvents` | `boolean` | Skip events without subscribers to reduce costs. Note: just for shared mode and locally. Do not use this in prod. | `false` | No |

## AWS Authentication

This plugin supports multiple AWS authentication methods:

1. **Environment Variables** (Recommended):
   ```bash
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_REGION=your_region
   ```

2. **IAM Roles** (When running on EC2/ECS):
   The plugin will automatically use the IAM role attached to the instance.

3. **Explicit Credentials**:
   ```javascript
   options: {
     queueUrl: "...",
     region: "...",
     useCredentials: true,
     accessKeyId: "your-access-key-id",
     secretAccessKey: "your-secret-access-key",
   }
   ```

## SQS Queue Configuration

For optimal performance, configure your SQS queue with these settings:

1. **Queue Type**: Standard Queue (recommended for most use cases)
2. **Message Retention Period**: At least 4 days (default)
3. **Visibility Timeout**: 30 seconds (adjust based on your event processing time)
4. **Receive Message Wait Time**: 20 seconds (for long polling)
5. **Dead-Letter Queue**: Configure to handle failed messages

## Troubleshooting

### Common Issues

1. **Authentication Errors**:
   - Ensure your AWS credentials are properly configured
   - Verify the IAM user/role has SQS permissions

2. **Queue URL Errors**:
   - Double-check the queue URL format
   - Ensure the queue exists in the specified region

3. **Message Processing Issues**:
   - Check your event subscribers for errors
   - Verify the visibility timeout is sufficient for your processing time

## Contributing

This is an unofficial community plugin. Contributions are welcome! Please feel free to submit issues and pull requests.

## License

MIT

## Disclaimer

This plugin is not officially supported by Medusa. Use at your own risk. For official Medusa event bus implementations, see the [Medusa documentation](https://docs.medusajs.com/).

## Related Packages

- [@medusajs/event-bus-redis](https://www.npmjs.com/package/@medusajs/event-bus-redis) - Official Redis event bus
- [@medusajs/event-bus-local](https://www.npmjs.com/package/@medusajs/event-bus-local) - Official local event bus
- [@medusajs/medusa](https://www.npmjs.com/package/@medusajs/medusa) - Core Medusa framework