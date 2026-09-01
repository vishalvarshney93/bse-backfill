targetScope = 'resourceGroup'

@description('Globally unique lowercase storage account name, 3-24 characters.')
param storageAccountName string

@description('Object ID of the GitHub Actions service principal. Leave blank to skip role assignments.')
param githubPrincipalId string = ''

@description('Object ID of a user or group allowed to inspect POC Blob and Table data. Leave blank to skip.')
param operatorReaderPrincipalId string = ''

@allowed([
  'User'
  'Group'
  'ServicePrincipal'
])
param operatorReaderPrincipalType string = 'User'

param location string = resourceGroup().location

var blobContributorRoleId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
var tableContributorRoleId = '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3'
var blobReaderRoleId = '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1'
var tableReaderRoleId = '76199698-9eea-4c19-bc75-cec21354c6b6'

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    accessTier: 'Hot'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    defaultToOAuthAuthentication: true
    minimumTlsVersion: 'TLS1_2'
    publicNetworkAccess: 'Enabled'
    supportsHttpsTrafficOnly: true
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
  properties: {
    deleteRetentionPolicy: {
      enabled: true
      days: 7
    }
  }
}

resource filingsContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: 'filings-md'
  properties: {
    publicAccess: 'None'
  }
}

resource researchContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: 'research-snapshots'
  properties: {
    publicAccess: 'None'
  }
}

resource tableService 'Microsoft.Storage/storageAccounts/tableServices@2023-05-01' = {
  parent: storage
  name: 'default'
}

resource ingestionState 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = {
  parent: tableService
  name: 'ResearchIngestionState'
}

resource lifecycle 'Microsoft.Storage/storageAccounts/managementPolicies@2023-05-01' = {
  parent: storage
  name: 'default'
  properties: {
    policy: {
      rules: [
        {
          enabled: true
          name: 'cool-historical-markdown'
          type: 'Lifecycle'
          definition: {
            actions: {
              baseBlob: {
                tierToCool: {
                  daysAfterModificationGreaterThan: 30
                }
              }
            }
            filters: {
              blobTypes: [
                'blockBlob'
              ]
              prefixMatch: [
                'filings-md/companies/'
              ]
            }
          }
        }
      ]
    }
  }
}

resource filingsBlobRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(githubPrincipalId)) {
  name: guid(filingsContainer.id, githubPrincipalId, blobContributorRoleId)
  scope: filingsContainer
  properties: {
    principalId: githubPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', blobContributorRoleId)
  }
}

resource researchBlobRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(githubPrincipalId)) {
  name: guid(researchContainer.id, githubPrincipalId, blobContributorRoleId)
  scope: researchContainer
  properties: {
    principalId: githubPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', blobContributorRoleId)
  }
}

resource tableRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(githubPrincipalId)) {
  name: guid(ingestionState.id, githubPrincipalId, tableContributorRoleId)
  scope: ingestionState
  properties: {
    principalId: githubPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', tableContributorRoleId)
  }
}

resource filingsOperatorReaderRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(operatorReaderPrincipalId)) {
  name: guid(filingsContainer.id, operatorReaderPrincipalId, blobReaderRoleId)
  scope: filingsContainer
  properties: {
    principalId: operatorReaderPrincipalId
    principalType: operatorReaderPrincipalType
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', blobReaderRoleId)
  }
}

resource researchOperatorReaderRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(operatorReaderPrincipalId)) {
  name: guid(researchContainer.id, operatorReaderPrincipalId, blobReaderRoleId)
  scope: researchContainer
  properties: {
    principalId: operatorReaderPrincipalId
    principalType: operatorReaderPrincipalType
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', blobReaderRoleId)
  }
}

resource tableOperatorReaderRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(operatorReaderPrincipalId)) {
  name: guid(ingestionState.id, operatorReaderPrincipalId, tableReaderRoleId)
  scope: ingestionState
  properties: {
    principalId: operatorReaderPrincipalId
    principalType: operatorReaderPrincipalType
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', tableReaderRoleId)
  }
}

output storageAccountName string = storage.name
output blobEndpoint string = storage.properties.primaryEndpoints.blob
output tableEndpoint string = storage.properties.primaryEndpoints.table
