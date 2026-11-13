mock_provider "azurerm" {
  mock_resource "azurerm_storage_account" {
    defaults = {
      id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-bootstrap/providers/Microsoft.Storage/storageAccounts/stpeoplecounterstate"
    }
  }
  mock_resource "azurerm_storage_container" {
    defaults = {
      id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-bootstrap/providers/Microsoft.Storage/storageAccounts/stpeoplecounterstate/blobServices/default/containers/tfstate"
    }
  }
}

mock_provider "azuread" {
  mock_data "azuread_client_config" {
    defaults = {
      object_id = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
    }
  }

  mock_resource "azuread_service_principal" {
    defaults = {
      object_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    }
  }
  mock_resource "azuread_application" {
    defaults = {
      id        = "/applications/ffffffff-ffff-ffff-ffff-ffffffffffff"
      client_id = "ffffffff-ffff-ffff-ffff-ffffffffffff"
    }
  }
}

mock_provider "random" {}

override_resource {
  target = azuread_service_principal.github_oidc["test"]
  values = { object_id = "dddddddd-dddd-dddd-dddd-dddddddddddd" }
}

variables {
  subscription_id                       = "00000000-0000-0000-0000-000000000000"
  tenant_id                             = "11111111-1111-1111-1111-111111111111"
  state_storage_account_name            = "stpeoplecountertest"
  github_oidc_subject_prefix            = "repo:owner@123/people-counter@456"
  environments                          = ["dev", "test"]
  adls_workspace_identity_principal_ids = {}
  adls_storage_by_environment = {
    dev = {
      mode                        = "existing"
      storage_account_resource_id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-data/providers/Microsoft.Storage/storageAccounts/stpeoplecounterdev"
      container_name              = "people-counter"
    }
    test = {
      mode                        = "existing"
      storage_account_resource_id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-data/providers/Microsoft.Storage/storageAccounts/stpeoplecountertest"
      container_name              = "people-counter"
    }
  }
}

run "no_identity_means_no_delegation" {
  command = apply

  assert {
    condition     = length(azurerm_role_assignment.adls_user_access_administrator) == 0 && length(azurerm_role_assignment.adls_reader) == 2
    error_message = "First-stage bootstrap must retain Reader but grant no role-assignment authority before workspace identities are known."
  }
}

run "constrain_create_and_delete_to_role_and_workspace" {
  command = apply

  variables {
    adls_workspace_identity_principal_ids = {
      dev  = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
      test = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    }
  }

  assert {
    condition = length(azurerm_role_assignment.adls_user_access_administrator) == 2 && alltrue([
      for environment, assignment in azurerm_role_assignment.adls_user_access_administrator :
      assignment.condition_version == "2.0"
      && assignment.scope == var.adls_storage_by_environment[environment].storage_account_resource_id
      && assignment.principal_id == azuread_service_principal.github_oidc[environment].object_id
      && assignment.role_definition_name == "User Access Administrator"
      && try(trimspace(replace(assignment.condition, "/\\s+/", " ")), "") == join(" ", [
        "((!(ActionMatches{'Microsoft.Authorization/roleAssignments/write'})) OR",
        "(@Request[Microsoft.Authorization/roleAssignments:RoleDefinitionId] ForAnyOfAnyValues:GuidEquals {ba92f5b4-2d11-453d-a403-e96b0029c9fe}",
        "AND @Request[Microsoft.Authorization/roleAssignments:PrincipalId] ForAnyOfAnyValues:GuidEquals {${var.adls_workspace_identity_principal_ids[environment]}}))",
        "AND ((!(ActionMatches{'Microsoft.Authorization/roleAssignments/delete'})) OR",
        "(@Resource[Microsoft.Authorization/roleAssignments:RoleDefinitionId] ForAnyOfAnyValues:GuidEquals {ba92f5b4-2d11-453d-a403-e96b0029c9fe}",
        "AND @Resource[Microsoft.Authorization/roleAssignments:PrincipalId] ForAnyOfAnyValues:GuidEquals {${var.adls_workspace_identity_principal_ids[environment]}}))",
      ])
    ])
    error_message = "Delegation must restrict both writes and deletes to the environment workspace principal AND Storage Blob Data Contributor."
  }
}

run "partial_identity_configuration" {
  command = apply

  variables {
    adls_workspace_identity_principal_ids = {
      dev = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    }
  }

  assert {
    condition     = toset(keys(azurerm_role_assignment.adls_user_access_administrator)) == toset(["dev"])
    error_message = "An environment without an approved workspace identity must not receive delegation."
  }
}

run "reject_invalid_identity" {
  command = plan

  variables {
    adls_workspace_identity_principal_ids = { dev = "not-a-guid" }
  }

  expect_failures = [var.adls_workspace_identity_principal_ids]
}

run "reject_unconfigured_environment" {
  command = plan

  variables {
    adls_workspace_identity_principal_ids = {
      prod = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    }
  }

  expect_failures = [var.adls_workspace_identity_principal_ids]
}

run "reject_zero_identity" {
  command = plan

  variables {
    adls_workspace_identity_principal_ids = {
      dev = "00000000-0000-0000-0000-000000000000"
    }
  }

  expect_failures = [var.adls_workspace_identity_principal_ids]
}

run "reject_self_grant" {
  command = plan

  variables {
    adls_workspace_identity_principal_ids = {
      dev = "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"
    }
  }

  expect_failures = [azurerm_role_assignment.adls_user_access_administrator["dev"]]
}

run "reject_other_deployment_principal" {
  command = plan

  variables {
    adls_workspace_identity_principal_ids = {
      dev = "dddddddd-dddd-dddd-dddd-dddddddddddd"
    }
  }

  expect_failures = [azurerm_role_assignment.adls_user_access_administrator["dev"]]
}
