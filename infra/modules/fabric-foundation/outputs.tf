output "workspace_id" {
  value = fabric_workspace.this.id
}

output "workspace_identity" {
  value = var.enable_workspace_identity ? fabric_workspace.this.identity : null
}

output "workspace_folder_ids" {
  description = "Workspace folder IDs keyed by caller-defined labels."
  value = {
    for key, folder in fabric_folder.this : key => folder.id
  }
}

output "lakehouse_id" {
  value = fabric_lakehouse.this.id
}

output "onelake_shortcut_ids" {
  value = {
    for key, shortcut in fabric_shortcut.onelake : key => shortcut.id
  }
}

output "onelake_shortcuts" {
  description = "Managed shortcut identities and non-secret targets keyed by caller label."
  value = {
    for key, shortcut in var.onelake_shortcuts : key => {
      id                  = fabric_shortcut.onelake[key].id
      name                = shortcut.name
      path                = shortcut.path
      target_item_id      = shortcut.target_item_id
      target_path         = shortcut.target_path
      target_workspace_id = shortcut.target_workspace_id
    }
  }
}

output "adls_shortcut_ids" {
  value = {
    for key, shortcut in fabric_shortcut.adls : key => shortcut.id
  }
}

output "adls_shortcuts" {
  description = "Managed ADLS shortcut identities and non-secret targets."
  value = {
    for key, shortcut in var.adls_shortcuts : key => {
      id            = fabric_shortcut.adls[key].id
      name          = shortcut.name
      path          = shortcut.path
      location      = local.adls_shortcut_targets[key].location
      subpath       = local.adls_shortcut_targets[key].subpath
      connection_id = fabric_connection.adls_workspace_identity[0].id
      container     = shortcut.container
      target_path   = shortcut.target_path
    }
  }
}

output "managed_shortcuts" {
  description = "All managed Lakehouse shortcuts keyed by target type and label."
  value = merge(
    {
      for key, shortcut in var.onelake_shortcuts : "onelake:${key}" => {
        target_type         = "OneLake"
        id                  = fabric_shortcut.onelake[key].id
        name                = shortcut.name
        path                = shortcut.path
        target_workspace_id = shortcut.target_workspace_id
        target_item_id      = shortcut.target_item_id
        target_path         = shortcut.target_path
        location            = null
        subpath             = null
        connection_id       = null
      }
    },
    {
      for key, shortcut in var.adls_shortcuts : "adls:${key}" => {
        target_type         = "AdlsGen2"
        id                  = fabric_shortcut.adls[key].id
        name                = shortcut.name
        path                = shortcut.path
        target_workspace_id = null
        target_item_id      = null
        target_path         = shortcut.target_path
        location            = local.adls_shortcut_targets[key].location
        subpath             = local.adls_shortcut_targets[key].subpath
        connection_id       = fabric_connection.adls_workspace_identity[0].id
      }
    },
  )
}

output "environment_id" {
  value = fabric_environment.this.id
}

output "variable_library_id" {
  description = "Deployment Variable Library ID, or null when disabled."
  value = (
    var.variable_library == null
    ? null
    : fabric_variable_library.deployment[0].id
  )
}

output "spark_job_definition_ids" {
  value = {
    for role, sjd in fabric_spark_job_definition.this : role => sjd.id
  }
}

output "connection_ids" {
  description = "Non-secret Fabric connection IDs keyed by caller-defined labels."
  value = {
    for key, connection in fabric_connection.this : key => connection.id
  }
}

output "adls_connection_id" {
  description = "WorkspaceIdentity ADLS connection ID, or null when disabled."
  value = (
    length(var.adls_shortcuts) == 0
    ? null
    : fabric_connection.adls_workspace_identity[0].id
  )
}

output "connection_role_assignment_ids" {
  description = "Connection role assignment IDs keyed by connection/assignment label."
  value = {
    for key, assignment in fabric_connection_role_assignment.this :
    key => assignment.id
  }
}

output "domain_id" {
  description = "Managed Fabric governance domain ID, or null when disabled."
  value       = var.domain == null ? null : fabric_domain.this[0].id
}

output "governance_tag_ids" {
  description = "Managed governance tag IDs keyed by caller-defined labels."
  value = {
    for key, tag in fabric_tag.this : key => tag.id
  }
}

output "onelake_data_access_roles" {
  description = "Managed OneLake data-access role names keyed by caller-defined labels."
  value = {
    for key, role in fabric_onelake_data_access_security.this :
    key => role.role_name
  }
}

output "semantic_model_ids" {
  description = "Semantic model item IDs keyed by reporting bundle."
  value = {
    for key, model in fabric_semantic_model.this : key => model.id
  }
}

output "report_ids" {
  description = "Report item IDs keyed by reporting bundle."
  value = {
    for key, report in fabric_report.this : key => report.id
  }
}

output "eventhouse_id" {
  value = local.rti_enabled ? fabric_eventhouse.operations[0].id : null
}

output "kql_database_id" {
  value = (
    local.rti_enabled
    ? fabric_kql_database.operations["operations"].id
    : null
  )
}

output "kql_query_service_uri" {
  value = (
    local.rti_enabled
    ? fabric_kql_database.operations["operations"].properties.query_service_uri
    : null
  )
}

output "kql_queryset_id" {
  value = (
    local.kql_queryset_definition == null
    ? null
    : fabric_kql_queryset.operations["operations"].id
  )
}

output "kql_dashboard_id" {
  value = (
    local.kql_dashboard_definition == null
    ? null
    : fabric_kql_dashboard.operations["operations"].id
  )
}

output "activator_ids" {
  value = {
    for key, activator in fabric_activator.this : key => activator.id
  }
}

output "deployment_pipeline_id" {
  value = (
    local.deployment_pipeline_enabled
    ? fabric_deployment_pipeline.this[0].id
    : null
  )
}

output "deployment_pipeline_role_assignment_ids" {
  description = "Deployment Pipeline Admin assignment IDs keyed by caller label."
  value = {
    for key, assignment in fabric_deployment_pipeline_role_assignment.this :
    key => assignment.id
  }
}

output "definition_display_names" {
  description = "Non-secret item names used to sanitize live definition exports."
  value = {
    workspace   = fabric_workspace.this.display_name
    lakehouse   = fabric_lakehouse.this.display_name
    environment = fabric_environment.this.display_name
    semanticModels = {
      for key, model in var.reporting_items :
      key => model.semantic_model_display_name
    }
    reports = {
      for key, report in var.reporting_items :
      key => report.report_display_name
    }
  }
}
