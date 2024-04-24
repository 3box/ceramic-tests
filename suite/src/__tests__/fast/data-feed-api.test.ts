import { describe, test, beforeAll, expect } from '@jest/globals'
import { newCeramic, waitForAnchor } from '../../utils/ceramicHelpers.js'
import { createDid } from '../../utils/didHelper.js'
import { EventAccumulator } from '../../utils/common.js'
import { StreamID } from '@ceramicnetwork/streamid'
import { Model } from '@ceramicnetwork/stream-model'
import { ModelInstanceDocument, ModelInstanceDocumentMetadataArgs } from '@ceramicnetwork/stream-model-instance'
import { newModel } from '../../models/modelConstants'
import { CeramicClient } from '@ceramicnetwork/http-client'
import { CommonTestUtils as TestUtils } from '@ceramicnetwork/common-test-utils'
import { EventSource } from 'cross-eventsource'
import { JsonAsString, AggregationDocument } from '@ceramicnetwork/codecs'
import { decode } from 'codeco'

const ComposeDbUrls = String(process.env.COMPOSEDB_URLS).split(',')
const adminSeeds = String(process.env.COMPOSEDB_ADMIN_DID_SEEDS).split(',')

describe('Datafeed SSE Api Test', () => {
  let ceramicNode1: CeramicClient
  let ceramicNode2: CeramicClient
  let modelId: StreamID
  let modelInstanceDocumentMetadata: ModelInstanceDocumentMetadataArgs
  let Codec: any
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

    modelInstanceDocumentMetadata = { model: modelId }
    Codec = JsonAsString.pipe(AggregationDocument)
  })

  test('event format is as expected', async () => {
    const source = new EventSource(
      new URL('/api/v0/feed/aggregation/documents', ComposeDbUrls[0]).toString(),
    )
    let event: any
    const parseEventData = (eventData: any) => {
      event = decode(Codec, eventData) // a single event is expected for this test scenario
      return event.commitId.commit.toString()
    }

    const accumulator = new EventAccumulator(source, parseEventData)
    
    try {
      const expectedEvents = new Set()
      // genesis commit
      const doc = await ModelInstanceDocument.create(
        ceramicNode1,
        { myData: 40 },
        modelInstanceDocumentMetadata,
      )
      expectedEvents.add(doc.tip.toString())

      await accumulator.waitForEvents(expectedEvents, 1000 * 60)
      const receivedEvent = [...accumulator.allEvents].pop()
      expect(receivedEvent).toHaveProperty("commitId")
      expect(receivedEvent).toHaveProperty("content")
      expect(receivedEvent).toHaveProperty("metadata")
      expect(receivedEvent).toHaveProperty("eventType")
    } finally {
      source.close()
    }
  })

  test('genesis and data commits are delivered', async () => {
    const source1 = new EventSource(
      new URL('/api/v0/feed/aggregation/documents', ComposeDbUrls[0]).toString(),
    )
    const source2 = new EventSource(
      new URL('/api/v0/feed/aggregation/documents', ComposeDbUrls[1]).toString(),
    )

    const parseEventData = (eventData: any) => {
      const decoded: any = decode(Codec, eventData)
      return decoded.commitId.commit.toString()
    }

    const accumulator1 = new EventAccumulator(source1, parseEventData)
    const accumulator2 = new EventAccumulator(source2, parseEventData)

    try {
      const expectedEvents = new Set()
      // genesis commits
      const document1 = await ModelInstanceDocument.create(
        ceramicNode1,
        { myData: 40 },
        modelInstanceDocumentMetadata,
      )
      expectedEvents.add(document1.tip.toString())

      const document2 = await ModelInstanceDocument.create(
        ceramicNode1,
        { myData: 50 },
        modelInstanceDocumentMetadata,
      )
      expectedEvents.add(document2.tip.toString())

      const document3 = await ModelInstanceDocument.create(
        ceramicNode1,
        { myData: 60 },
        modelInstanceDocumentMetadata,
      )
      expectedEvents.add(document3.tip.toString())
      // data commits
      await document1.replace({ myData: 41 })
      expectedEvents.add(document1.tip.toString())
      await document2.replace({ myData: 51 })
      expectedEvents.add(document2.tip.toString())
      await document1.replace({ myData: 42 })
      expectedEvents.add(document1.tip.toString())
      await accumulator1.waitForEvents(expectedEvents, 1000 * 60)
      await accumulator2.waitForEvents(expectedEvents, 1000 * 60)

      expect(accumulator1.allEvents).toBe(expectedEvents)
      expect(accumulator2.allEvents).toBe(expectedEvents)
    } finally {
      source1.close()
      source2.close()
    }
  })

  test('time commits are delivered', async () => {
    const source = new EventSource(
      new URL('/api/v0/feed/aggregation/documents', ComposeDbUrls[0]).toString(),
    )

    const parseEventData = (eventData: any) => {
      const decoded: any = decode(Codec, eventData)
      return decoded.commitId.commit.toString()
    }

    const accumulator = new EventAccumulator(source, parseEventData)
    
    try {
      const expectedEvents = new Set()
      // genesis commit
      const doc = await ModelInstanceDocument.create(
        ceramicNode1,
        { myData: 40 },
        modelInstanceDocumentMetadata,
      )
      expectedEvents.add(doc.tip.toString())

      // time commit
      await waitForAnchor(doc).catch((errStr) => {
        throw new Error(errStr)
      })
      expectedEvents.add(doc.tip.toString())
      await accumulator.waitForEvents(expectedEvents, 1000 * 60)

      expect(accumulator.allEvents).toBe(expectedEvents)
    } finally {
      source.close()
    }
  })
  // this wont be tested until the feature its ready
  test.skip('if a connection goes offline can resume the missed events upon reconnection', async () => {
    const source = new EventSource(
      new URL('/api/v0/feed/aggregation/documents', ComposeDbUrls[0]).toString(),
    )

    const parseEventData = (eventData: any) => {
      const decoded: any = decode(Codec, eventData)
      return decoded.commitId.commit.toString()
    }

    const accumulator = new EventAccumulator(source, parseEventData)
    
    try {
      const expectedEvents = new Set()
      // genesis commit
      const doc = await ModelInstanceDocument.create(
        ceramicNode1,
        { myData: 40 },
        modelInstanceDocumentMetadata,
      )
      expectedEvents.add(doc.tip.toString())

      // data commit offline
      await doc.replace({ myData: 41 })
      expectedEvents.add(doc.tip.toString())
      await accumulator.waitForEvents(expectedEvents, 1000 * 60)

      expect(accumulator.allEvents).toBe(expectedEvents)
    } finally {
      source.close()
    }
  })
})
