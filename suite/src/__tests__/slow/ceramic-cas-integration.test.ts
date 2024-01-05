import { AnchorStatus, CeramicApi, SyncOptions } from '@ceramicnetwork/common'
import { TileDocument } from '@ceramicnetwork/stream-tile'
import { jest, describe, test, beforeAll, expect } from '@jest/globals'
import { newCeramic, waitForAnchor } from '../../utils/ceramicHelpers.js'

const ComposeDbUrls = String(process.env.COMPOSEDB_URLS).split(',')

describe('Ceramic<->CAS basic integration', () => {
  jest.setTimeout(1000 * 60 * 60) // 1 hour
  let ceramic: CeramicApi
  let ceramicWrongPubsub: CeramicApi

  beforeAll(async () => {
    ceramic = await newCeramic(ComposeDbUrls[0])
    ceramicWrongPubsub = await newCeramic(ComposeDbUrls[ComposeDbUrls.length - 1])
  })

  test('basic crud is anchored properly, single update per anchor batch', async () => {
    // Test document creation
    console.log('Creating document')
    const initialContent = { foo: 'bar' }
    const doc = await TileDocument.create<any>(ceramic, initialContent)
    expect(doc.content).toEqual(initialContent)

    // Test document creation is anchored correctly
    console.log('Waiting for anchor of genesis record')
    await waitForAnchor(doc).catch((errStr) => {
      throw new Error(errStr)
    })
    expect(doc.state.log.length).toEqual(2)

    // Test document update
    console.log('Updating document')
    const newContent = { bar: 'baz' }
    await doc.update(newContent)
    expect(doc.content).toEqual(newContent)

    // Test document update is anchored correctly
    console.log('Waiting for anchor of update')
    await waitForAnchor(doc).catch((errStr) => {
      throw new Error(errStr)
    })
    expect(doc.content).toEqual(newContent)
    expect(doc.state.log.length).toEqual(4)
  })

  test('multiple documents are anchored properly, multiple updates per anchor batch', async () => {
    const content0 = { state: 0 }
    const content1 = { state: 1 }
    const content2 = { state: 2 }
    const content3 = { state: 3 }
    const content4 = { state: 4 }

    // Create some documents
    console.log('Creating documents')
    const doc1 = await TileDocument.create(ceramic, content0)
    const doc2 = await TileDocument.create(ceramic, content0)
    const doc3 = await TileDocument.create(ceramic, content0)
    const doc4 = await TileDocument.create(ceramic, content0)
    expect(doc1.content).toEqual(content0)
    expect(doc2.content).toEqual(content0)
    expect(doc3.content).toEqual(content0)
    expect(doc4.content).toEqual(content0)

    // Test document creation is anchored correctly
    console.log('Waiting for anchor of genesis records')
    await waitForAnchor(doc1).catch((errStr) => {
      throw new Error(errStr)
    })
    await waitForAnchor(doc2).catch((errStr) => {
      throw new Error(errStr)
    })
    await waitForAnchor(doc3).catch((errStr) => {
      throw new Error(errStr)
    })
    await waitForAnchor(doc4).catch((errStr) => {
      throw new Error(errStr)
    })

    expect(doc1.state.anchorStatus).toEqual(AnchorStatus.ANCHORED)
    expect(doc1.state.log.length).toEqual(2)
    expect(doc2.state.anchorStatus).toEqual(AnchorStatus.ANCHORED)
    expect(doc2.state.log.length).toEqual(2)
    expect(doc3.state.anchorStatus).toEqual(AnchorStatus.ANCHORED)
    expect(doc3.state.log.length).toEqual(2)
    expect(doc4.state.anchorStatus).toEqual(AnchorStatus.ANCHORED)
    expect(doc4.state.log.length).toEqual(2)

    // Test document updates
    console.log('Updating documents')
    await doc1.update(content1, undefined, { anchor: true })
    await doc2.update(content1, undefined, { anchor: false })
    await doc3.update(content1, undefined, { anchor: false })
    await doc4.update(content1, undefined, { anchor: false })

    await doc2.update(content2, undefined, { anchor: true })
    await doc3.update(content2, undefined, { anchor: false })
    await doc4.update(content2, undefined, { anchor: false })

    await doc3.update(content3, undefined, { anchor: true })
    await doc4.update(content3, undefined, { anchor: false })

    await doc4.update(content4, undefined, { anchor: true })

    expect(doc1.content).toEqual(content1)
    expect(doc2.content).toEqual(content2)
    expect(doc3.content).toEqual(content3)
    expect(doc4.content).toEqual(content4)

    // Test document updates are anchored correctly
    console.log('Waiting for anchor of updates')
    await waitForAnchor(doc1).catch((errStr) => {
      throw new Error(errStr)
    })
    await waitForAnchor(doc2).catch((errStr) => {
      throw new Error(errStr)
    })
    await waitForAnchor(doc3).catch((errStr) => {
      throw new Error(errStr)
    })
    await waitForAnchor(doc4).catch((errStr) => {
      throw new Error(errStr)
    })

    expect(doc1.state.anchorStatus).toEqual(AnchorStatus.ANCHORED)
    expect(doc2.state.anchorStatus).toEqual(AnchorStatus.ANCHORED)
    expect(doc3.state.anchorStatus).toEqual(AnchorStatus.ANCHORED)
    expect(doc4.state.anchorStatus).toEqual(AnchorStatus.ANCHORED)
    expect(doc1.content).toEqual(content1)
    expect(doc2.content).toEqual(content2)
    expect(doc3.content).toEqual(content3)
    expect(doc4.content).toEqual(content4)
    expect(doc1.state.log.length).toEqual(4)
    expect(doc2.state.log.length).toEqual(5)
    expect(doc3.state.log.length).toEqual(6)
    expect(doc4.state.log.length).toEqual(7)
  })

  if (ComposeDbUrls.length > 2) {
    test('Can retrieve anchor commit for doc that was created by another node that is unavailable', async () => {
      // create a doc with a node using the wrong pubsub. This update should not get propagated to other nodes.
      const content = { state: 0 }
      const doc = await TileDocument.create(ceramicWrongPubsub, content, undefined, {
        anchor: true,
      })
      expect(doc.content).toEqual(content)
      expect(doc.state.anchorStatus).toEqual(AnchorStatus.PENDING)

      await waitForAnchor(doc).catch((errStr) => {
        throw new Error(errStr)
      })
      expect(doc.state.anchorStatus).toEqual(AnchorStatus.ANCHORED)

      // loading the document without querying the network
      const loaded = await TileDocument.load(ceramic, doc.id, { sync: SyncOptions.NEVER_SYNC })
      expect(loaded.content).toEqual(content)
      expect(loaded.state.anchorStatus).toEqual(AnchorStatus.NOT_REQUESTED)

      // since creamicWrongPubsub is on the incorrect pubsub, the only way ceramic can get the update is from the pubsub responder
      await loaded.sync({ sync: SyncOptions.SYNC_ALWAYS })
      await waitForAnchor(loaded).catch((errStr) => {
        throw new Error(errStr)
      })
      expect(loaded.content).toEqual(content)
      expect(loaded.state.anchorStatus).toEqual(AnchorStatus.ANCHORED)
    })
  }
})
