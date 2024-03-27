import { ComposeClient } from '@composedb/client'
import { utilities } from './common'

const delay = utilities.delay

export async function waitForDocument(
  composeClient: ComposeClient,
  query: string,
  variables: any,
  timeoutMs: number,
) {
  const startTime = Date.now()
  while (true) {
    const response = await composeClient.executeQuery(query, variables)
    const responseObj = JSON.parse(JSON.stringify(response))
    if (responseObj?.data?.node?.id) {
      return responseObj
    }
    if (Date.now() - startTime > timeoutMs) {
      throw new Error('Timeout waiting for document')
    }
    await delay(1)
  }
}
