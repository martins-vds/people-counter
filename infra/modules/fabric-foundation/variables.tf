variable "workspace_display_name" {
  description = "Display name for the Fabric workspace."
  type        = string

  validation {
    condition     = length(trimspace(var.workspace_display_name)) > 0
    error_message = "workspace_display_name must not be empty."
  }
}

variable "workspace_description" {
  description = "Description for the Fabric workspace."
  type        = string
  default     = ""
}

variable "capacity_id" {
  description = "Fabric capacity ID assigned to the workspace."
  type        = string

  validation {
    condition = can(regex(
      "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
      var.capacity_id,
    ))
    error_message = "capacity_id must be a GUID."
  }
}

variable "skip_capacity_state_validation" {
  description = "Skip capacity validation only when the caller cannot list capacities."
  type        = bool
  default     = false
}

variable "enable_workspace_identity" {
  description = "Assign a system identity to the workspace."
  type        = bool
  default     = true
}

variable "workspace_role_assignments" {
  description = "Workspace role assignments keyed by a stable label."
  type = map(object({
    principal_id   = string
    principal_type = string
    role           = string
  }))
  default = {}

  validation {
    condition = alltrue([
      for assignment in var.workspace_role_assignments :
      contains(["Admin", "Contributor", "Member", "Viewer"], assignment.role)
    ])
    error_message = "role must be Admin, Contributor, Member, or Viewer."
  }

  validation {
    condition = alltrue([
      for assignment in var.workspace_role_assignments :
      contains(
        ["User", "Group", "ServicePrincipal", "ServicePrincipalProfile"],
        assignment.principal_type,
      )
    ])
    error_message = "principal_type is not supported."
  }

  validation {
    condition = alltrue([
      for assignment in var.workspace_role_assignments :
      can(regex(
        "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
        assignment.principal_id,
      ))
    ])
    error_message = "Every workspace role principal_id must be a GUID."
  }
}

variable "lakehouse_display_name" {
  description = "Display name for the Fabric Lakehouse."
  type        = string

  validation {
    condition     = length(trimspace(var.lakehouse_display_name)) > 0
    error_message = "lakehouse_display_name must not be empty."
  }
}

variable "lakehouse_description" {
  description = "Description for the Fabric Lakehouse."
  type        = string
  default     = ""
}

variable "environment_display_name" {
  description = "Display name for the Fabric Environment."
  type        = string

  validation {
    condition     = length(trimspace(var.environment_display_name)) > 0
    error_message = "environment_display_name must not be empty."
  }
}

variable "environment_description" {
  description = "Description for the Fabric Environment."
  type        = string
  default     = ""
}

variable "spark_job_definitions" {
  description = "Names and descriptions for the seven canonical SJD roles."
  type = map(object({
    display_name = string
    description  = optional(string, "")
  }))

  validation {
    condition = toset(keys(var.spark_job_definitions)) == toset([
      "control",
      "dispatcher",
      "process",
      "refresh",
      "reconciliation",
      "gold",
      "cutover",
    ])
    error_message = "spark_job_definitions must contain the seven canonical roles."
  }

}

variable "reporting_items" {
  description = "Target semantic model and report names keyed by analytics/operational."
  type = map(object({
    semantic_model_display_name = string
    report_display_name         = string
  }))

  validation {
    condition = toset(keys(var.reporting_items)) == toset([
      "analytics",
      "operational",
    ])
    error_message = "reporting_items must contain analytics and operational."
  }
}

variable "deployment_pipeline" {
  description = "Optional three-stage Fabric Deployment Pipeline."
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

  validation {
    condition = (
      var.deployment_pipeline == null
      ? true
      : length(var.deployment_pipeline.stages) == 3
    )
    error_message = "deployment_pipeline must declare exactly three stages."
  }

  validation {
    condition = (
      var.deployment_pipeline == null
      ? true
      : length(trimspace(var.deployment_pipeline.display_name)) > 0
      && alltrue([
        for stage in var.deployment_pipeline.stages :
        length(trimspace(stage.display_name)) > 0
      ])
      && alltrue([
        for assignment in var.deployment_pipeline.role_assignments :
        can(regex(
          "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
          assignment.principal_id,
        ))
        && contains([
          "Group",
          "ServicePrincipal",
          "ServicePrincipalProfile",
          "User",
        ], assignment.principal_type)
        && assignment.role == "Admin"
      ])
    )
    error_message = "Deployment pipeline names and Admin role assignments must satisfy provider contracts."
  }
}

variable "tags" {
  description = "Fabric tag IDs applied to provisioned items."
  type        = set(string)
  default     = []
}

variable "enable_activator" {
  description = "Deploy the preview-isolated source-controlled Activator definition."
  type        = bool
  default     = false
}

variable "onelake_shortcuts" {
  description = "OneLake shortcuts created inside the managed Lakehouse."
  type = map(object({
    name                = string
    path                = string
    target_workspace_id = string
    target_item_id      = string
    target_path         = string
  }))
  default = {}

  validation {
    condition = alltrue([
      for shortcut in var.onelake_shortcuts :
      !startswith(shortcut.path, "/")
      && !startswith(shortcut.target_path, "/")
      && (
        startswith(shortcut.path, "Files/")
        || startswith(shortcut.path, "Tables/")
      )
      && (
        startswith(shortcut.target_path, "Files/")
        || startswith(shortcut.target_path, "Tables/")
      )
    ])
    error_message = "Shortcut paths must start with Files/ or Tables/ and must not start with '/'."
  }

  validation {
    condition = alltrue(flatten([
      for shortcut in var.onelake_shortcuts : [
        can(regex(
          "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
          shortcut.target_workspace_id,
        )),
        can(regex(
          "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
          shortcut.target_item_id,
        )),
        length(trimspace(shortcut.name)) > 0,
      ]
    ]))
    error_message = "Shortcut names must not be empty and target IDs must be GUIDs."
  }
}

variable "enable_preview_features" {
  description = "Permit resources that provider 1.14 marks as preview."
  type        = bool
  default     = false
}

variable "workspace_folders" {
  description = "Optional root-level workspace folders. Requires provider preview mode."
  type = map(object({
    display_name = string
  }))
  default = {}

  validation {
    condition = alltrue([
      for folder in var.workspace_folders :
      length(trimspace(folder.display_name)) > 0
      && length(folder.display_name) <= 255
      && !can(regex("[/~\"#%&*:<>?\\\\{|}]", folder.display_name))
    ])
    error_message = "Folder names must be 1-255 characters and must not contain Fabric-invalid characters."
  }
}

variable "variable_library" {
  description = "Source-controlled, non-secret deployment Variable Library."
  type = object({
    display_name     = string
    description      = optional(string, "")
    deployment_stage = string
    folder_key       = optional(string)
  })
  default = null

  validation {
    condition = var.variable_library == null ? true : (
      length(trimspace(var.variable_library.display_name)) > 0
      && contains(["dev", "test", "prod"], var.variable_library.deployment_stage)
      && (
        var.variable_library.folder_key == null
        || contains(keys(var.workspace_folders), var.variable_library.folder_key)
      )
    )
    error_message = "Variable Library needs a name, a dev/test/prod stage, and a declared folder_key when set."
  }
}

variable "non_secret_connections" {
  description = "Fabric cloud connections using only Anonymous or WorkspaceIdentity credentials."
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

  validation {
    condition = alltrue([
      for connection in var.non_secret_connections :
      length(trimspace(connection.display_name)) > 0
      && length(trimspace(connection.connection_type)) > 0
      && length(trimspace(connection.creation_method)) > 0
      && contains(["Anonymous", "WorkspaceIdentity"], connection.credential_type)
      && contains(["ShareableCloud", "VirtualNetworkGateway"], connection.connectivity_type)
      && contains(["None", "Organizational", "Private", "Public"], connection.privacy_level)
      && contains(["Any", "Encrypted", "NotEncrypted"], connection.connection_encryption)
      && contains([
        "Kerberos",
        "KerberosDirectQueryAndRefresh",
        "MicrosoftEntraID",
        "None",
        "SecurityAssertionMarkupLanguage",
      ], connection.single_sign_on_type)
    ])
    error_message = "Connection values must use provider-supported non-secret credential and enum values."
  }

  validation {
    condition = alltrue([
      for connection in var.non_secret_connections :
      connection.connectivity_type == "VirtualNetworkGateway"
      ? (
        connection.gateway_id != null
        && can(regex(
          "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
          connection.gateway_id,
        ))
        && !connection.allow_connection_usage_in_gateway
        && !connection.allow_usage_in_user_controlled_code
      )
      : connection.gateway_id == null
    ])
    error_message = "VirtualNetworkGateway connections require a gateway GUID and null-only usage flags; ShareableCloud connections must omit gateway_id."
  }

  validation {
    condition = alltrue(flatten([
      for connection in var.non_secret_connections : [
        for assignment in connection.role_assignments :
        can(regex(
          "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
          assignment.principal_id,
        ))
        && contains([
          "EntireTenant",
          "Group",
          "ServicePrincipal",
          "ServicePrincipalProfile",
          "User",
        ], assignment.principal_type)
        && contains(["Owner", "User", "UserWithReshare"], assignment.role)
      ]
    ]))
    error_message = "Connection role assignments require GUID principal IDs and provider-supported principal/role values."
  }
}

variable "domain" {
  description = "Optional Fabric governance domain. Creation requires Fabric administrator authority."
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

  validation {
    condition = var.domain == null ? true : (
      length(trimspace(var.domain.display_name)) > 0
      && length(var.domain.display_name) <= 40
      && length(var.domain.description) <= 256
      && (
        var.domain.default_label_id == null
        || can(regex(
          "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
          var.domain.default_label_id,
        ))
      )
      && alltrue(flatten([
        for assignment in var.domain.role_assignments : [
          contains(["Admins", "Contributors"], assignment.role),
          length(assignment.principals) > 0,
          alltrue([
            for principal in assignment.principals :
            contains(["EntireTenant", "Group", "User"], principal.type)
            && (
              principal.type == "EntireTenant"
              || can(regex(
                "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
                principal.id,
              ))
            )
          ])
        ]
      ]))
    )
    error_message = "Domain values violate provider name, GUID, role, or principal contracts."
  }
}

variable "governance_tags" {
  description = "Tags created at tenant or managed-domain scope and applied to managed items."
  type = map(object({
    display_name = string
    scope        = optional(string, "Tenant")
  }))
  default = {}

  validation {
    condition = alltrue([
      for tag in var.governance_tags :
      length(trimspace(tag.display_name)) > 0
      && length(tag.display_name) <= 40
      && contains(["Tenant", "Domain"], tag.scope)
      && (tag.scope != "Domain" || var.domain != null)
    ])
    error_message = "Tags require 1-40 character names; Domain-scoped tags require the managed domain."
  }
}

variable "onelake_data_access_roles" {
  description = "Preview OneLake data-access roles for the managed Lakehouse."
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

  validation {
    condition = alltrue([
      for access_role in var.onelake_data_access_roles :
      length(trimspace(access_role.role_name)) > 0
      && (
        length(access_role.members.microsoft_entra_members) > 0
        || length(access_role.members.fabric_item_members) > 0
      )
      && length(access_role.decision_rules) > 0
      && alltrue([
        for member in access_role.members.microsoft_entra_members :
        contains(["Group", "ManagedIdentity", "ServicePrincipal", "User"], member.object_type)
        && can(regex(
          "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
          member.object_id,
        ))
        && can(regex(
          "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
          member.tenant_id,
        ))
      ])
      && alltrue([
        for rule in access_role.decision_rules :
        rule.effect == "Permit"
        && length(rule.permission) > 0
        && alltrue([
          for permission in rule.permission :
          contains(["Action", "Path"], permission.attribute_name)
          && length(permission.attribute_value_included_in) > 0
        ])
      ])
    ])
    error_message = "OneLake roles require valid names, members, GUIDs, and provider-supported Permit rules."
  }
}
