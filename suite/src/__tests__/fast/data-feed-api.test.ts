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
import { utilities } from '../../utils/common.js'

const delay = utilities.delay
const ComposeDbUrls = String(process.env.COMPOSEDB_URLS)?.split(',')
const adminSeeds = String(process.env.COMPOSEDB_ADMIN_DID_SEEDS).split(',')
const nodeSyncWaitTimeSec = 9

async function genesisCommit(ceramicNode: CeramicClient, modelInstanceDocumentMetadata: ModelInstanceDocumentMetadataArgs, anchor: boolean) {
  return await ModelInstanceDocument.create(
    ceramicNode,
    { myData: Math.floor(Math.random() * (100)) + 1},
    modelInstanceDocumentMetadata,
    { anchor }
  )
}

describe('Datafeed SSE Api Test', () => {
  let ceramicNode1: CeramicClient
  let ceramicNode2: CeramicClient
  let modelId: StreamID
  let modelInstanceDocumentMetadata: ModelInstanceDocumentMetadataArgs
  let Codec: any
  beforeAll(async () => {
    if(!ComposeDbUrls)
    throw new Error(
      'ComposeDbUrls expects a minimum of 2 urls, but none were found',
    )

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
    console.log(`${modelId} is the model id used in the data feed tests`)
    modelInstanceDocumentMetadata = { model: modelId }
    Codec = JsonAsString.pipe(AggregationDocument)
  })

  test('event format is as expected', async () => {
    let event: any
    const source = new EventSource(
      new URL('/api/v0/feed/aggregation/documents', ComposeDbUrls[0]).toString(),
    )
    const parseEventData = (eventData: any) => {
      event = decode(Codec, eventData) // a single event is expected for this test scenario
      return event.commitId.commit.toString()
    }

    const accumulator = new EventAccumulator(source, parseEventData)

    try {
      const expectedEvents = new Set()
      // genesis commit
      const doc = await genesisCommit(ceramicNode1, modelInstanceDocumentMetadata, false)
      expectedEvents.add(doc.tip.toString())

      await accumulator.waitForEvents(expectedEvents, 1000 * 60 * 2)

      expect(event).toHaveProperty("commitId")
      expect(event).toHaveProperty("content")
      expect(event).toHaveProperty("metadata")
      expect(event).toHaveProperty("eventType")
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
        { anchor: false }
      )
      expectedEvents.add(document1.tip.toString())
      console.log(`${document1.id} is the stream id for document 1 and ${document1.tip.toString()} is the genesis for it`)

      const document2 = await ModelInstanceDocument.create(
        ceramicNode1,
        { myData: 50 },
        modelInstanceDocumentMetadata,
        { anchor: false }
      )
      expectedEvents.add(document2.tip.toString())
      console.log(`${document2.id} is the stream id for document 2 and ${document2.tip.toString()} is the genesis for it`)

       // data commits
       await document1.replace({ myData: 41 }, null, { anchor: false })
       expectedEvents.add(document1.tip.toString())
       console.log(`${document1.tip.toString()} is the update for doc1`)
       await document2.replace({ myData: 51 }, null, { anchor: false })
       expectedEvents.add(document2.tip.toString())
       console.log(`${document2.tip.toString()} is the update for doc2`)
       // By waiting for the expected events we confirm the api delivers all events
       const start =  Date.now()
       await accumulator1.waitForEvents(expectedEvents, 1000 * 60 * 2)
       console.log(`${Date.now() - start} is the time we used getting the events for node 1`)
       await delay(nodeSyncWaitTimeSec)
       await accumulator2.waitForEvents(expectedEvents, 1000 * 60 * 2)
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
      const doc = await genesisCommit(ceramicNode1, modelInstanceDocumentMetadata, true)
      expectedEvents.add(doc.tip.toString())

      // time commit
      await waitForAnchor(doc).catch((errStr) => {
        throw new Error(errStr)
      })
      expectedEvents.add(doc.tip.toString())
      // By waiting for the expected events we confirm the api delivers all events)
      await accumulator.waitForEvents(expectedEvents, 1000 * 60 * 2)
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
      const doc = await genesisCommit(ceramicNode1, modelInstanceDocumentMetadata, false)
      expectedEvents.add(doc.tip.toString())
      // disconnect
      accumulator.stop()
      // data commit offline
      await doc.replace({ myData: 41 })
      expectedEvents.add(doc.tip.toString())

      // connection after events
      accumulator.start()

      await accumulator.waitForEvents(expectedEvents, 1000 * 60)

      expect(accumulator.allEvents).toBe(expectedEvents)
    } finally {
      source.close()
    }
  })
})
