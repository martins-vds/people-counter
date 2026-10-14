output "resource_group_name" {
  value = azurerm_resource_group.bootstrap.name
}

output "state_storage_account_name" {
  value = azurerm_storage_account.state.name
}

output "state_container_name" {
  value = azurerm_storage_container.state.name
}

output "backend_config_by_environment" {
  value = {
    for environment in var.environments : environment => {
      resource_group_name  = azurerm_resource_group.bootstrap.name
      storage_account_name = azurerm_storage_account.state.name
      container_name       = azurerm_storage_container.state.name
      key                  = "people-counter/${environment}.tfstate"
      use_azuread_auth     = true
    }
  }
}

output "github_oidc_client_ids" {
  value = {
    for environment, app in azuread_application.github_oidc :
    environment => app.client_id
  }
}

output "github_oidc_service_principal_object_ids" {
  value = {
    for environment, principal in azuread_service_principal.github_oidc :
    environment => principal.object_id
  }
}

output "fabric_capacity_ids_by_environment" {
  value = {
    for environment, capacity in azurerm_fabric_capacity.environment :
    environment => capacity.id
  }
}

output "adls_shortcut_source_by_environment" {
  description = "ADLS Gen2 storage account and container for each environment."
  value = {
    for environment, container in azurerm_storage_container.adls :
    environment => {
      storage_account_resource_id = local.adls_storage_account_ids[environment]
      container_name              = container.name
    }
  }
}

output "tenant_id" {
  value = var.tenant_id
}

output "subscription_id" {
  value = var.subscription_id
}
