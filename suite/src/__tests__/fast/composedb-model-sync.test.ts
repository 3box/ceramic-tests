import { ComposeClient } from '@composedb/client'
import { beforeAll, describe, test, expect } from '@jest/globals'
import { Composite } from '@composedb/devtools'
import { newCeramic } from '../../utils/ceramicHelpers.js'
import { createDid } from '../../utils/didHelper.js'
import { BasicSchema } from '../../graphql-schemas/basicSchema'
import { StreamID } from '@ceramicnetwork/streamid'
import { waitForDocument } from '../../utils/composeDbHelpers.js'

const ComposeDbUrls = String(process.env.COMPOSEDB_URLS).split(',')
const adminSeeds = String(process.env.COMPOSEDB_ADMIN_DID_SEEDS).split(',')
const timeoutMs = 60000

describe('Sync Model and ModelInstanceDocument using ComposeDB GraphQL API', () => {
  let composeClient1: ComposeClient
  let composeClient2: ComposeClient

  beforeAll(async () => {
    const did1 = await createDid(adminSeeds[0])
    if (!adminSeeds[1])
      throw new Error(
        'adminSeeds expects minimum 2 dids one for each url, adminSeeds[1] is not set',
      )
    const did2 = await createDid(adminSeeds[1])

    const ceramicInstance1 = await newCeramic(ComposeDbUrls[0], did1)
    const ceramicInstance2 = await newCeramic(ComposeDbUrls[1], did2)

    const composite = await Composite.create({ ceramic: ceramicInstance1, schema: BasicSchema })
    composeClient1 = await new ComposeClient({
      ceramic: ceramicInstance1,
      definition: composite.toRuntime(),
    })
    composeClient1.setDID(did1)

    // CACAO resources URLs for the models the client interacts with
    const resources = composeClient1.resources
    // Fetch themodelId of the model created by the node
    const parts = String(resources[0]).split('model=')
    const modelId = parts[parts.length - 1]

    // start indexing for tha nodel on node 2
    await ceramicInstance2.admin.startIndexingModels([StreamID.fromString(modelId)])
    composeClient2 = await new ComposeClient({
      ceramic: ceramicInstance2,
      definition: composite.toRuntime(),
    })
    composeClient2.setDID(did2)
  })

  test('Create and sync a ModelInstanceDocument', async () => {
    const createDocumentMutation = `
        mutation createBasicSchema($input: CreateBasicSchemaInput!) {
            createBasicSchema(input: $input) {
            document {
                id
                myData
            }
            }
        }
        `

    const createDocumentVariables = {
      input: {
        content: {
          myData: 'my data' + Date.now(),
        },
      },
    }

    const response = await composeClient1.executeQuery(
      createDocumentMutation,
      createDocumentVariables,
    )
    const responseObject = await JSON.parse(JSON.stringify(response))
    const documentId = responseObject?.data?.createBasicSchema?.document?.id
    expect(documentId).toBeDefined()

    const getDocumentByStreamIdQuery = `
    query GetBasicSchemaById($id: ID!) {
        node(id: $id) {
          ... on BasicSchema {
            id
            myData
          }
        }
      }
        `

    const getDocumentByStreamIdVariables = {
      id: documentId,
    }

    const queryResponse = await waitForDocument(
      composeClient2,
      getDocumentByStreamIdQuery,
      getDocumentByStreamIdVariables,
      timeoutMs,
    )
    const queryResponseObj = JSON.parse(JSON.stringify(queryResponse))
    const queryResponseid = queryResponseObj?.data?.node?.id
    expect(queryResponseid).toBeDefined()
  })
})
