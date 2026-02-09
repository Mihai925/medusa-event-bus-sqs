import {
  EventBusTypes,
  InternalModuleDeclaration,
  Logger,
  MedusaContainer,
  Message,
} from "@medusajs/framework/types"
import { AbstractEventBusModuleService } from "@medusajs/framework/utils"
import {
  SQSClient,
  SendMessageCommand,
  SendMessageBatchCommand,
  SendMessageBatchRequestEntry,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  Message as SQSMessage,
} from "@aws-sdk/client-sqs"

export type EventBusSqsOptions = {
  queueUrl: string
  region: string
  accessKeyId?: string
  secretAccessKey?: string
  sessionToken?: string
  useCredentials?: boolean
  waitTimeSeconds?: number
  pollingWaitTimeMs?: number
  maxNumberOfMessages?: number
  skipUnsubscribedEvents?: boolean
}

type InjectedDependencies = {
  logger: Logger
}

type StagingQueueType = Map<string, Message[]>

export default class SqsEventBusService extends AbstractEventBusModuleService {
  protected readonly logger_: Logger
  protected readonly sqsClient_: SQSClient
  protected readonly queueUrl_: string
  protected readonly options_: EventBusSqsOptions
  protected groupedEventsMap_: StagingQueueType
  protected isPolling_: boolean
  protected pollingTimeoutRef_: ReturnType<typeof setTimeout> | null

  constructor(
    { logger }: MedusaContainer & InjectedDependencies,
    moduleOptions: EventBusSqsOptions,
    moduleDeclaration: InternalModuleDeclaration
  ) {
    // @ts-expect-error - super expects generic args
    super(...arguments)

    this.logger_ = logger ?? console
    this.options_ = moduleOptions
    this.queueUrl_ = moduleOptions.queueUrl
    this.groupedEventsMap_ = new Map()
    this.isPolling_ = false
    this.pollingTimeoutRef_ = null

    const clientConfig: Record<string, unknown> = {
      region: moduleOptions.region,
    }

    if (
      moduleOptions.useCredentials &&
      moduleOptions.accessKeyId &&
      moduleOptions.secretAccessKey
    ) {
      clientConfig.credentials = {
        accessKeyId: moduleOptions.accessKeyId,
        secretAccessKey: moduleOptions.secretAccessKey,
        ...(moduleOptions.sessionToken && {
          sessionToken: moduleOptions.sessionToken,
        }),
      }
    }

    this.sqsClient_ = new SQSClient(clientConfig)

    this.startPolling()
  }

  async emit<T = unknown>(
    eventsData: Message<T> | Message<T>[],
    options: Record<string, unknown> = {}
  ): Promise<void> {
    const normalizedEventsData = Array.isArray(eventsData)
      ? eventsData
      : [eventsData]

    const eventsToSend: Message<T>[] = []

    // Only for shared modes. We can skip events that have no subscribers to reduce
    // unnecessary SQS messages. For non-shared modes, we should always send the events. 
    const skipUnsubscribed = this.options_.skipUnsubscribedEvents ?? false

    for (const eventData of normalizedEventsData) {
      if (skipUnsubscribed) {
        const subscribers = this.eventToSubscribersMap_.get(eventData.name) || []
        const starSubscribers = this.eventToSubscribersMap_.get("*") || []
        if (subscribers.length === 0 && starSubscribers.length === 0) {
          continue
        }
      }

      const eventGroupId = eventData.metadata?.eventGroupId
      if (eventGroupId) {
        await this.groupEvent_(eventGroupId, eventData)
      } else {
        eventsToSend.push(eventData)
      }
    }

    if (eventsToSend.length === 0) {
      return
    }

    if (eventsToSend.length === 1) {
      const event = eventsToSend[0]
      const command = new SendMessageCommand({
        QueueUrl: this.queueUrl_,
        MessageBody: JSON.stringify({
          name: event.name,
          data: event.data,
          metadata: event.metadata,
        }),
      })

      await this.sqsClient_.send(command)
    } else {
      // Batch send in groups of 10 (SQS max)
      for (let i = 0; i < eventsToSend.length; i += 10) {
        const batch = eventsToSend.slice(i, i + 10)
        const entries: SendMessageBatchRequestEntry[] = batch.map(
          (event, idx) => ({
            Id: `${i + idx}`,
            MessageBody: JSON.stringify({
              name: event.name,
              data: event.data,
              metadata: event.metadata,
            }),
          })
        )

        const command = new SendMessageBatchCommand({
          QueueUrl: this.queueUrl_,
          Entries: entries,
        })

        await this.sqsClient_.send(command)
      }
    }
  }

  private async groupEvent_<T = unknown>(
    eventGroupId: string,
    eventData: Message<T>
  ) {
    const groupedEvents = this.groupedEventsMap_.get(eventGroupId) || []
    groupedEvents.push(eventData as Message)
    this.groupedEventsMap_.set(eventGroupId, groupedEvents)
  }

  async releaseGroupedEvents(eventGroupId: string): Promise<void> {
    const groupedEvents = this.groupedEventsMap_.get(eventGroupId) || []
    const eventsToEmit: Message[] = JSON.parse(JSON.stringify(groupedEvents))

    await this.clearGroupedEvents(eventGroupId)

    if (eventsToEmit.length === 0) {
      return
    }

    // Remove the eventGroupId so they don't get grouped again
    const eventsWithoutGroupId = eventsToEmit.map((event) => {
      const { eventGroupId: _, ...restMetadata } = event.metadata || {}
      return {
        ...event,
        metadata: restMetadata,
      }
    }) as Message[]

    await this.emit(eventsWithoutGroupId)
  }

  async clearGroupedEvents(
    eventGroupId: string,
    { eventNames }: { eventNames?: string[] } = {}
  ): Promise<void> {
    if (eventNames?.length) {
      const groupedEvents = this.groupedEventsMap_.get(eventGroupId) || []
      const eventsToKeep = groupedEvents.filter(
        (event) => !eventNames.includes(event.name)
      )
      this.groupedEventsMap_.set(eventGroupId, eventsToKeep)
    } else {
      this.groupedEventsMap_.delete(eventGroupId)
    }
  }

  startPolling(): void {
    if (this.isPolling_) {
      return
    }

    this.isPolling_ = true
    this.logger_.info("event-bus-sqs: Starting SQS polling")
    this.poll_()
  }

  stopPolling(): void {
    this.isPolling_ = false

    if (this.pollingTimeoutRef_) {
      clearTimeout(this.pollingTimeoutRef_)
      this.pollingTimeoutRef_ = null
    }

    this.logger_.info("event-bus-sqs: Stopped SQS polling")
  }

  private async poll_(): Promise<void> {
    if (!this.isPolling_) {
      return
    }

    try {
      const command = new ReceiveMessageCommand({
        QueueUrl: this.queueUrl_,
        WaitTimeSeconds: this.options_.waitTimeSeconds ?? 20,
        MaxNumberOfMessages: this.options_.maxNumberOfMessages ?? 10,
      })

      const response = await this.sqsClient_.send(command)
      const messages = response.Messages || []

      for (const sqsMessage of messages) {
        await this.processMessage_(sqsMessage)
      }
    } catch (error: any) {
      this.logger_.error("event-bus-sqs: Error polling SQS queue")
      this.logger_.error(error)
    }

    if (this.isPolling_) {
      const delay = this.options_.pollingWaitTimeMs ?? 0
      if (delay > 0) {
        this.pollingTimeoutRef_ = setTimeout(() => this.poll_(), delay)
      } else {
        // Use setImmediate to avoid blocking the event loop
        this.pollingTimeoutRef_ = setTimeout(() => this.poll_(), 0)
      }
    }
  }

  private async processMessage_(sqsMessage: SQSMessage): Promise<void> {
    if (!sqsMessage.Body) {
      return
    }

    try {
      const parsed = JSON.parse(sqsMessage.Body) as {
        name: string
        data: unknown
        metadata: Record<string, unknown>
      }

      const eventName = parsed.name
      const eventBody = {
        name: parsed.name,
        data: parsed.data,
        metadata: parsed.metadata,
      }

      // Dispatch to local subscribers
      const subscribers = this.eventToSubscribersMap_.get(eventName) || []
      const starSubscribers = this.eventToSubscribersMap_.get("*") || []
      const allSubscribers = [...subscribers, ...starSubscribers]

      const totalSubscribers = allSubscribers.length

      if (totalSubscribers > 0) {
        this.logger_.info(
          `Processing ${eventName} which has ${totalSubscribers} subscribers`
        )
      }

      for (const { subscriber } of allSubscribers) {
        try {
          await subscriber(eventBody)
        } catch (err: any) {
          this.logger_.error(
            `An error occurred while processing ${eventName}:`
          )
          this.logger_.error(err)
        }
      }

      // Delete the message after processing
      if (sqsMessage.ReceiptHandle) {
        const deleteCommand = new DeleteMessageCommand({
          QueueUrl: this.queueUrl_,
          ReceiptHandle: sqsMessage.ReceiptHandle,
        })

        await this.sqsClient_.send(deleteCommand)
      }
    } catch (err: any) {
      this.logger_.error("event-bus-sqs: Error processing message")
      this.logger_.error(err)
    }
  }
}
