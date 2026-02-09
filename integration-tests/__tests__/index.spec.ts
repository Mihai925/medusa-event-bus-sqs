import SqsEventBusService from "../../src/services/event-bus-sqs"

// Mock the SQS client
const mockSend = jest.fn().mockResolvedValue({})

jest.mock("@aws-sdk/client-sqs", () => {
  return {
    SQSClient: jest.fn().mockImplementation(() => ({
      send: mockSend,
    })),
    SendMessageCommand: jest.fn().mockImplementation((input) => ({
      _type: "SendMessageCommand",
      input,
    })),
    SendMessageBatchCommand: jest.fn().mockImplementation((input) => ({
      _type: "SendMessageBatchCommand",
      input,
    })),
    ReceiveMessageCommand: jest.fn().mockImplementation((input) => ({
      _type: "ReceiveMessageCommand",
      input,
    })),
    DeleteMessageCommand: jest.fn().mockImplementation((input) => ({
      _type: "DeleteMessageCommand",
      input,
    })),
  }
})

function createEventBus() {
  const logger = {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  }

  const service = new SqsEventBusService(
    { logger } as any,
    {
      queueUrl: "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
      region: "us-east-1",
    },
    { worker_mode: "worker" } as any
  )

  return { service, logger }
}

describe("Event Bus SQS Service", () => {
  beforeEach(() => {
    jest.clearAllMocks()
    mockSend.mockResolvedValue({})
  })

  it("should emit an event", async () => {
    const { service } = createEventBus()

    await service.emit({
      name: "test",
      data: { test: "test" },
      metadata: {
        source: "test",
        object: "test",
        action: "created",
      },
    })

    expect(mockSend).toHaveBeenCalledTimes(1)

    const sentCommand = mockSend.mock.calls[0][0]
    expect(sentCommand._type).toBe("SendMessageCommand")

    const body = JSON.parse(sentCommand.input.MessageBody)
    expect(body).toEqual({
      name: "test",
      data: { test: "test" },
      metadata: {
        source: "test",
        object: "test",
        action: "created",
      },
    })
  })

  it("should release grouped events", async () => {
    const { service } = createEventBus()

    // Emit with eventGroupId - should NOT send to SQS
    await service.emit({
      name: "test",
      data: { test: "test" },
      metadata: {
        source: "test",
        object: "test",
        action: "created",
        eventGroupId: "123",
      },
    })

    expect(mockSend).toHaveBeenCalledTimes(0)

    // Release grouped events - should now send to SQS
    await service.releaseGroupedEvents("123")

    expect(mockSend).toHaveBeenCalledTimes(1)

    const sentCommand = mockSend.mock.calls[0][0]
    const body = JSON.parse(sentCommand.input.MessageBody)
    expect(body).toEqual({
      name: "test",
      data: { test: "test" },
      metadata: {
        source: "test",
        object: "test",
        action: "created",
      },
    })
  })

  it("should clear grouped events", async () => {
    const { service } = createEventBus()

    await service.emit({
      name: "test",
      data: { test: "test" },
      metadata: {
        source: "test",
        object: "test",
        action: "created",
        eventGroupId: "123",
      },
    })

    expect(mockSend).toHaveBeenCalledTimes(0)

    await service.clearGroupedEvents("123")
    await service.releaseGroupedEvents("123")

    // Nothing should have been sent since events were cleared
    expect(mockSend).toHaveBeenCalledTimes(0)
  })

  it("should clear grouped events with event names", async () => {
    const { service } = createEventBus()

    await service.emit({
      name: "test",
      data: { test: "test" },
      metadata: {
        source: "test",
        object: "test",
        action: "created",
        eventGroupId: "123",
      },
    })

    await service.clearGroupedEvents("123", {
      eventNames: ["test"],
    })

    await service.releaseGroupedEvents("123")

    // Nothing should have been sent since events were cleared by name
    expect(mockSend).toHaveBeenCalledTimes(0)
  })

  it("should batch emit events in groups of 10", async () => {
    const { service } = createEventBus()

    const events = Array.from({ length: 15 }, (_, i) => ({
      name: `test-${i}`,
      data: { index: i },
      metadata: { source: "test", object: "test", action: "created" },
    }))

    await service.emit(events)

    // Should send 2 batch commands (10 + 5)
    expect(mockSend).toHaveBeenCalledTimes(2)
    expect(mockSend.mock.calls[0][0]._type).toBe("SendMessageBatchCommand")
    expect(mockSend.mock.calls[0][0].input.Entries).toHaveLength(10)
    expect(mockSend.mock.calls[1][0]._type).toBe("SendMessageBatchCommand")
    expect(mockSend.mock.calls[1][0].input.Entries).toHaveLength(5)
  })

  it("should process received SQS messages and dispatch to subscribers", async () => {
    const { service } = createEventBus()

    const subscriber = jest.fn()
    service.subscribe("order.placed", subscriber)

    // Simulate processing a message
    await (service as any).processMessage_({
      Body: JSON.stringify({
        name: "order.placed",
        data: { id: "order_123" },
        metadata: { source: "order", action: "created" },
      }),
      ReceiptHandle: "test-receipt-handle",
    })

    expect(subscriber).toHaveBeenCalledWith({
      name: "order.placed",
      data: { id: "order_123" },
      metadata: { source: "order", action: "created" },
    })

    // Should have called DeleteMessage
    expect(mockSend).toHaveBeenCalledTimes(1)
    expect(mockSend.mock.calls[0][0]._type).toBe("DeleteMessageCommand")
  })
})
