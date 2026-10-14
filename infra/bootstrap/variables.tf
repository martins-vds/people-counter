variable "subscription_id" {
  description = "Azure subscription ID that hosts the bootstrap resources."
  type        = string
}

variable "tenant_id" {
  description = "Microsoft Entra tenant ID used by the AzureRM and AzureAD providers."
  type        = string
}

variable "location" {
  description = "Azure region for state storage and Fabric capacities."
  type        = string
  default     = "westeurope"
}

variable "resource_group_name" {
  description = "Resource group holding bootstrap resources."
  type        = string
  default     = "rg-people-counter-bootstrap"
}

variable "state_storage_account_name" {
  description = "Globally unique state storage account name."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]{3,24}$", var.state_storage_account_name))
    error_message = "state_storage_account_name must be 3-24 lowercase letters/numbers."
  }
}

variable "state_storage_replication_type" {
  description = "Replication type for Terraform state storage."
  type        = string
  default     = "ZRS"
}

variable "state_container_name" {
  description = "Blob container holding environment state files."
  type        = string
  default     = "tfstate"
}

variable "github_repository" {
  description = "GitHub repository in owner/repo form."
  type        = string

  validation {
    condition     = can(regex("^[^/]+/[^/]+$", var.github_repository))
    error_message = "github_repository must be in owner/repo form."
  }
}

variable "environments" {
  description = "GitHub Environments receiving isolated OIDC identities."
  type        = list(string)
  default     = ["dev", "test", "prod"]
}

variable "fabric_capacity_sku_by_environment" {
  description = "Fabric capacity SKU per environment."
  type        = map(string)
  default = {
    dev  = "F2"
    test = "F4"
    prod = "F64"
  }
}

variable "fabric_admin_members" {
  description = "Additional capacity administrators."
  type        = list(string)
  default     = []
}

variable "adls_storage_by_environment" {
  description = "New or existing ADLS Gen2 storage account configuration per environment."
  type = map(object({
    mode                             = string
    storage_account_name             = optional(string)
    storage_account_resource_id      = optional(string)
    storage_account_replication_type = optional(string, "ZRS")
  }))
  default = {}

  validation {
    condition = alltrue([
      for environment, storage in var.adls_storage_by_environment :
      contains(var.environments, environment)
      && contains(["create", "existing"], storage.mode)
      && (
        storage.mode == "create"
        ? (
          storage.storage_account_name != null
          && can(regex("^[a-z0-9]{3,24}$", storage.storage_account_name))
          && storage.storage_account_resource_id == null
        )
        : (
          storage.storage_account_name == null
          && storage.storage_account_resource_id != null
          && can(regex(
            "(?i)^/subscriptions/[0-9a-f-]{36}/resourceGroups/[^/]+/providers/Microsoft\\.Storage/storageAccounts/[a-z0-9]{3,24}$",
            storage.storage_account_resource_id,
          ))
        )
      )
    ])
    error_message = "Each ADLS entry must select create with a valid account name or existing with a storage account resource ID."
  }
}

variable "tags" {
  description = "Common resource tags."
  type        = map(string)
  default = {
    purpose   = "terraform-bootstrap"
    project   = "people-counter"
    managedBy = "terraform"
  }
}
