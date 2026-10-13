data "azuread_client_config" "current" {}

resource "random_id" "fabric_suffix" {
  byte_length = 2
}

resource "azurerm_resource_group" "bootstrap" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_storage_account" "state" {
  name                = var.state_storage_account_name
  resource_group_name = azurerm_resource_group.bootstrap.name
  location            = azurerm_resource_group.bootstrap.location

  account_kind                    = "StorageV2"
  account_tier                    = "Standard"
  account_replication_type        = var.state_storage_replication_type
  min_tls_version                 = "TLS1_2"
  public_network_access           = "Enabled"
  shared_access_key_enabled       = false
  allow_nested_items_to_be_public = false

  blob_properties {
    versioning_enabled = true

    delete_retention_policy {
      days = 30
    }

    container_delete_retention_policy {
      days = 30
    }
  }

  tags = var.tags
}

resource "azurerm_storage_container" "state" {
  name                  = var.state_container_name
  storage_account_id    = azurerm_storage_account.state.id
  container_access_type = "private"
}

resource "azuread_application" "github_oidc" {
  for_each = toset(var.environments)

  display_name = "people-counter-${each.key}-github-oidc"
  owners       = [data.azuread_client_config.current.object_id]
}

resource "azuread_service_principal" "github_oidc" {
  for_each = azuread_application.github_oidc

  client_id = each.value.client_id
  owners    = [data.azuread_client_config.current.object_id]
}

resource "azuread_application_federated_identity_credential" "github_environment" {
  for_each = azuread_application.github_oidc

  application_id = each.value.id
  display_name   = "github-environment-${each.key}"
  description    = "GitHub Actions OIDC trust for ${each.key}"
  audiences      = ["api://AzureADTokenExchange"]
  issuer         = "https://token.actions.githubusercontent.com"
  subject        = "repo:${var.github_repository}:environment:${each.key}"
}

resource "azurerm_role_assignment" "state_blob_data_contributor" {
  for_each = azuread_service_principal.github_oidc

  scope                = azurerm_storage_container.state.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = each.value.object_id
}

resource "azurerm_fabric_capacity" "environment" {
  for_each = toset(var.environments)

  name                = "fabpc${each.key}${random_id.fabric_suffix.hex}"
  resource_group_name = azurerm_resource_group.bootstrap.name
  location            = azurerm_resource_group.bootstrap.location

  administration_members = distinct(concat(
    [azuread_service_principal.github_oidc[each.key].object_id],
    var.fabric_admin_members,
  ))

  sku {
    name = lookup(var.fabric_capacity_sku_by_environment, each.key, "F2")
    tier = "Fabric"
  }

  tags = merge(var.tags, { environment = each.key })
}
