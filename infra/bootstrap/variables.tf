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

variable "tags" {
  description = "Common resource tags."
  type        = map(string)
  default = {
    purpose   = "terraform-bootstrap"
    project   = "people-counter"
    managedBy = "terraform"
  }
}
