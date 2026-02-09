import { ModuleExports } from "@medusajs/framework/types"
import Loader from "./loaders"
import SqsEventBusService from "./services/event-bus-sqs"

export const service = SqsEventBusService
export const loaders = [Loader]

const moduleDefinition: ModuleExports = {
  service,
  loaders,
}

export default moduleDefinition
export * from "./initialize"
