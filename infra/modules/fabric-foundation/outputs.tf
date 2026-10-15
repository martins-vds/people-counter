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

output "adls_shortcut_id" {
  value = fabric_shortcut.adls.id
}

output "adls_shortcut" {
  description = "Managed ADLS shortcut identity and non-secret target."
  value = {
    id            = fabric_shortcut.adls.id
    name          = var.adls_shortcut_name
    path          = var.adls_shortcut_path
    location      = local.adls_location
    subpath       = local.adls_subpath
    connection_id = fabric_connection.adls_workspace_identity.id
    container     = var.adls_shortcut_container
    target_path   = var.adls_shortcut_target_path
  }
}

output "managed_shortcuts" {
  description = "The managed ADLS Gen2 shortcut."
  value = {
    adls = {
      target_type   = "AdlsGen2"
      id            = fabric_shortcut.adls.id
      name          = var.adls_shortcut_name
      path          = var.adls_shortcut_path
      target_path   = var.adls_shortcut_target_path
      location      = local.adls_location
      subpath       = local.adls_subpath
      connection_id = fabric_connection.adls_workspace_identity.id
    }
  }
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
  description = "WorkspaceIdentity ADLS connection ID."
  value       = fabric_connection.adls_workspace_identity.id
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
  description = "Workspace Monitoring Eventhouse item ID used by RTI definitions."
  value = (
    local.rti_enabled
    ? data.fabric_kql_database.workspace_monitoring[0].properties.eventhouse_id
    : null
  )
}

output "kql_database_id" {
  description = "Workspace Monitoring KQL database item ID used by RTI definitions."
  value = (
    local.rti_enabled
    ? data.fabric_kql_database.workspace_monitoring[0].id
    : null
  )
}

output "kql_query_service_uri" {
  description = "Workspace Monitoring KQL query service URI used by RTI definitions."
  value = (
    local.rti_enabled
    ? data.fabric_kql_database.workspace_monitoring[0].properties.query_service_uri
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
