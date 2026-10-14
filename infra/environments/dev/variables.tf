variable "capacity_id" {
  type = string

  validation {
    condition = can(regex(
      "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
      var.capacity_id,
    ))
    error_message = "capacity_id must be a GUID."
  }
}

variable "workspace_role_assignments" {
  type = map(object({
    principal_id   = string
    principal_type = string
    role           = string
  }))
  default = {}
}

variable "deployment_pipeline" {
  type = object({
    display_name = string
    description  = optional(string, "")
    stages = list(object({
      display_name = string
      description  = string
      is_public    = optional(bool, false)
    }))
    role_assignments = optional(map(object({
      principal_id   = string
      principal_type = string
      role           = optional(string, "Admin")
    })), {})
  })
  default = null
}

variable "enable_activator" {
  type    = bool
  default = false
}

variable "adls_storage_account_resource_id" {
  type = string
}

variable "adls_shortcut_name" {
  type = string
}

variable "adls_shortcut_path" {
  type = string
}

variable "adls_shortcut_container" {
  type = string
}

variable "adls_shortcut_target_path" {
  type = string
}

variable "enable_preview_features" {
  type    = bool
  default = false
}

variable "workspace_folders" {
  type = map(object({
    display_name = string
  }))
  default = {}
}

variable "non_secret_connections" {
  type = map(object({
    display_name                        = string
    connection_type                     = string
    creation_method                     = string
    parameters                          = optional(map(string), {})
    credential_type                     = string
    connectivity_type                   = optional(string, "ShareableCloud")
    gateway_id                          = optional(string)
    privacy_level                       = optional(string, "Organizational")
    connection_encryption               = optional(string, "Encrypted")
    single_sign_on_type                 = optional(string, "None")
    skip_test_connection                = optional(bool, false)
    allow_connection_usage_in_gateway   = optional(bool, false)
    allow_usage_in_user_controlled_code = optional(bool, false)
    role_assignments = optional(map(object({
      principal_id   = string
      principal_type = string
      role           = string
    })), {})
  }))
  default = {}
}

variable "domain" {
  type = object({
    display_name     = string
    description      = optional(string, "")
    default_label_id = optional(string)
    assign_workspace = optional(bool, false)
    role_assignments = optional(map(object({
      role = string
      principals = set(object({
        id   = string
        type = string
      }))
    })), {})
  })
  default = null
}

variable "governance_tags" {
  type = map(object({
    display_name = string
    scope        = optional(string, "Tenant")
  }))
  default = {}
}

variable "onelake_data_access_roles" {
  type = map(object({
    role_name = string
    members = object({
      microsoft_entra_members = optional(set(object({
        object_id   = string
        object_type = string
        tenant_id   = string
      })), [])
      fabric_item_members = optional(set(object({
        source_path = string
        item_access = set(string)
      })), [])
    })
    decision_rules = set(object({
      effect = string
      permission = set(object({
        attribute_name              = string
        attribute_value_included_in = set(string)
      }))
      constraints = optional(object({
        columns = optional(set(object({
          table_path    = string
          column_names  = set(string)
          column_effect = string
          column_action = set(string)
        })), [])
        rows = optional(set(object({
          table_path = string
          value      = string
        })), [])
      }))
    }))
  }))
  default = {}
}
