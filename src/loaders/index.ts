import { LoaderOptions } from "@medusajs/framework/types"

type EventBusSqsOptions = {
  queueUrl: string
  region: string
}

export default async ({
  logger,
  options,
}: LoaderOptions<EventBusSqsOptions>): Promise<void> => {
  if (!options?.queueUrl) {
    throw new Error(
      "event-bus-sqs: `queueUrl` is required in module options"
    )
  }

  if (!options?.region) {
    throw new Error(
      "event-bus-sqs: `region` is required in module options"
    )
  }

  logger?.info(`event-bus-sqs: Using queue ${options.queueUrl}`)
  logger?.info(`event-bus-sqs: Region ${options.region}`)
}
