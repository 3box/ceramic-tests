import { describe, test, beforeAll, expect } from '@jest/globals'
import { newCeramic } from '../../utils/ceramicHelpers.js'
import { createDid } from '../../utils/didHelper.js'
import { StreamID } from '@ceramicnetwork/streamid'
import { Model } from '@ceramicnetwork/stream-model'
import { ModelInstanceDocument } from '@ceramicnetwork/stream-model-instance'
import { newModel, basicModelDocumentContent } from '../../models/modelConstants'
import { CeramicClient } from '@ceramicnetwork/http-client'
import { CommonTestUtils as TestUtils } from '@ceramicnetwork/common-test-utils'
import { loadDocumentOrTimeout, waitForIndexingOrTimeout } from '../../utils/composeDbHelpers.js'

const ComposeDbUrls = String(process.env.COMPOSEDB_URLS).split(',')
const adminSeeds = String(process.env.COMPOSEDB_ADMIN_DID_SEEDS).split(',')
const nodeSyncWaitTimeSec = 5
const indexWaitTimeMin = 1

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
    await waitForIndexingOrTimeout(ceramicNode1, modelId, 1000 * 60 * indexWaitTimeMin)
    await waitForIndexingOrTimeout(ceramicNode2, modelId, 1000 * 60 * indexWaitTimeMin)
    modelId = model.id
  })

  test('Create a ModelInstanceDocument on one node and read it from another', async () => {
    const modelInstanceDocumentMetadata = { model: modelId }
    const document1 = await ModelInstanceDocument.create(
      ceramicNode1,
      basicModelDocumentContent,
      modelInstanceDocumentMetadata,
    )
    const document2 = await loadDocumentOrTimeout(
      ceramicNode2,
      document1.id,
      1000 * nodeSyncWaitTimeSec,
    )
    expect(document2.id).toEqual(document1.id)
  })
})
