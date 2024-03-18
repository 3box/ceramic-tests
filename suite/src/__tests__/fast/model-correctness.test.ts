import { describe, test, beforeAll, expect } from '@jest/globals'
import { newCeramic } from '../../utils/ceramicHelpers.js'
import { createDid } from '../../utils/didHelper.js'
import { EventAccumulator } from '../../utils/common.js'
import { StreamID } from '@ceramicnetwork/streamid'
import { Model } from '@ceramicnetwork/stream-model'
import { ModelInstanceDocument } from '@ceramicnetwork/stream-model-instance'
import { newModel, basicModelDocumentContent } from '../../models/modelConstants'
import { CeramicClient } from '@ceramicnetwork/http-client'
import { CommonTestUtils as TestUtils } from '@ceramicnetwork/common-test-utils'
import { utilities } from '../../utils/common.js'
import { EventSource } from 'cross-eventsource'
import { JsonAsString, AggregationDocument } from '@ceramicnetwork/codecs'
import { decode } from 'codeco'

const delay = utilities.delay
const ComposeDbUrls = String(process.env.COMPOSEDB_URLS).split(',')
const adminSeeds = String(process.env.COMPOSEDB_ADMIN_DID_SEEDS).split(',')
const nodeSyncWaitTimeSec = 2

describe('Model Integration Test', () => {
  let ceramicNode1: CeramicClient
  let ceramicNode2: CeramicClient
  let modelId: StreamID
  beforeAll(async () => {
    const did1 = await createDid(adminSeeds[0])
    if (!adminSeeds[1])
      throw new Error(
        'adminSeeds expects minimum 2 dids one for each url, adminSeeds[1] is not set',
      )
    const did2 = await createDid(adminSeeds[1])
    ceramicNode1 = await newCeramic(ComposeDbUrls[0], did1)
    ceramicNode2 = await newCeramic(ComposeDbUrls[1], did2)
    const model = await Model.create(ceramicNode1, newModel)
    await TestUtils.waitForConditionOrTimeout(async () =>
      ceramicNode2
        .loadStream(model.id)
        .then((_) => true)
        .catch((_) => false),
    )
    await ceramicNode1.admin.startIndexingModels([model.id])
    await ceramicNode2.admin.startIndexingModels([model.id])
    modelId = model.id
  })

  test('Create a ModelInstanceDocument on one node and read it from another', async () => {
    const modelInstanceDocumentMetdata = { model: modelId }
    const document1 = await ModelInstanceDocument.create(
      ceramicNode1,
      basicModelDocumentContent,
      modelInstanceDocumentMetdata,
    )
    // We have to wait for some time for sync to happen
    await delay(nodeSyncWaitTimeSec)
    const document2 = await ModelInstanceDocument.load(ceramicNode2, document1.id)
    expect(document2.id).toEqual(document1.id)
  })

  test('Model instance document changes are delivered via the datafeed SSE Api', async () => {
    const modelInstanceDocumentMetdata = { model: modelId }
    const Codec = JsonAsString.pipe(AggregationDocument)

    const source1 = new EventSource(
      new URL('/api/v0/feed/aggregation/documents', ComposeDbUrls[0]).toString(),
    )
    const source2 = new EventSource(
      new URL('/api/v0/feed/aggregation/documents', ComposeDbUrls[1]).toString(),
    )

    const parseEventData = (eventData: any) => {
      const decoded = decode(Codec, eventData)
      return decoded.commitId.commit.toString()
    }

    const accumulator1 = new EventAccumulator(source1, parseEventData)
    const accumulator2 = new EventAccumulator(source2, parseEventData)

    try {
      const expectedEvents = new Set()
      const document1 = await ModelInstanceDocument.create(
        ceramicNode1,
        { myData: 40 },
        modelInstanceDocumentMetdata,
      )
      expectedEvents.add(document1.tip.toString())

      const document2 = await ModelInstanceDocument.create(
        ceramicNode1,
        { myData: 50 },
        modelInstanceDocumentMetdata,
      )
      expectedEvents.add(document2.tip.toString())

      const document3 = await ModelInstanceDocument.create(
        ceramicNode1,
        { myData: 60 },
        modelInstanceDocumentMetdata,
      )
      expectedEvents.add(document3.tip.toString())

      await document1.replace({ myData: 41 })
      expectedEvents.add(document1.tip.toString())
      await document2.replace({ myData: 51 })
      expectedEvents.add(document2.tip.toString())
      await document1.replace({ myData: 42 })
      expectedEvents.add(document1.tip.toString())

      await accumulator1.waitForEvents(expectedEvents, 1000 * 60)
      await accumulator2.waitForEvents(expectedEvents, 1000 * 60)
    } finally {
      source1.close()
      source2.close()
    }
  })
})
