locals {
  deployment_pipeline_enabled = var.deployment_pipeline != null
  spark_job_definitions_root  = "${path.module}/../../../fabric/spark-job-definitions"
  fabric_content_root         = "${path.module}/../../../fabric"
  variable_library_root       = "${path.module}/definitions/variable-library"
  adls_storage_account_name   = basename(var.adls_storage_account_resource_id)
  adls_location               = "https://${local.adls_storage_account_name}.dfs.core.windows.net"
  adls_subpath = format(
    "/%s",
    join("/", compact([
      var.adls_shortcut_container,
      trim(var.adls_shortcut_target_path, "/"),
    ])),
  )

  connection_role_assignments = {
    for assignment in flatten([
      for connection_key, connection in var.non_secret_connections : [
        for assignment_key, role_assignment in connection.role_assignments : {
          key            = "${connection_key}/${assignment_key}"
          connection_key = connection_key
          principal_id   = role_assignment.principal_id
          principal_type = role_assignment.principal_type
          role           = role_assignment.role
        }
      ]
    ]) : assignment.key => assignment
  }

  semantic_model_candidates = {
    analytics = {
      display_name    = var.reporting_items.analytics.semantic_model_display_name
      description     = "Stable people-counter SJD analytics semantic model"
      definition_root = "${local.fabric_content_root}/semantic-models/analytics"
    }
    operational = {
      display_name    = var.reporting_items.operational.semantic_model_display_name
      description     = "Stable people-counter SJD operational semantic model"
      definition_root = "${local.fabric_content_root}/semantic-models/operational"
    }
  }
  semantic_models = {
    for key, model in local.semantic_model_candidates :
    key => merge(model, {
      metadata = jsondecode(file("${model.definition_root}/.fabric-item.json"))
    })
    if fileexists("${model.definition_root}/.fabric-item.json")
  }
  report_candidates = {
    analytics = {
      display_name    = var.reporting_items.analytics.report_display_name
      description     = "Stable people-counter SJD analytics report"
      definition_root = "${local.fabric_content_root}/reports/analytics"
    }
    operational = {
      display_name    = var.reporting_items.operational.report_display_name
      description     = "Stable people-counter SJD operational report"
      definition_root = "${local.fabric_content_root}/reports/operational"
    }
  }
  reports = {
    for key, report in local.report_candidates :
    key => merge(report, {
      metadata = jsondecode(file("${report.definition_root}/.fabric-item.json"))
    })
    if(
      contains(keys(local.semantic_models), key)
      && fileexists("${report.definition_root}/.fabric-item.json")
    )
  }

  eventhouse_root = "${local.fabric_content_root}/rti/eventhouse"
  eventhouse_definition = fileexists("${local.eventhouse_root}/.fabric-item.json") ? {
    definition_root = local.eventhouse_root
    metadata        = jsondecode(file("${local.eventhouse_root}/.fabric-item.json"))
  } : null
  kql_database_root = "${local.fabric_content_root}/rti/kql-database"
  kql_database_definition = fileexists("${local.kql_database_root}/.fabric-item.json") ? {
    definition_root = local.kql_database_root
    metadata        = jsondecode(file("${local.kql_database_root}/.fabric-item.json"))
  } : null
  kql_queryset_root = "${local.fabric_content_root}/rti/queryset"
  kql_queryset_definition = fileexists("${local.kql_queryset_root}/.fabric-item.json") ? {
    definition_root = local.kql_queryset_root
    metadata        = jsondecode(file("${local.kql_queryset_root}/.fabric-item.json"))
  } : null
  kql_querysets = local.kql_queryset_definition == null ? {} : {
    operations = local.kql_queryset_definition
  }
  kql_dashboard_root = "${local.fabric_content_root}/rti/dashboard"
  kql_dashboard_definition = fileexists("${local.kql_dashboard_root}/.fabric-item.json") ? {
    definition_root = local.kql_dashboard_root
    metadata        = jsondecode(file("${local.kql_dashboard_root}/.fabric-item.json"))
  } : null
  kql_dashboards = local.kql_dashboard_definition == null ? {} : {
    operations = local.kql_dashboard_definition
  }
  rti_enabled = anytrue([
    local.eventhouse_definition != null,
    local.kql_database_definition != null,
    local.kql_queryset_definition != null,
    local.kql_dashboard_definition != null,
  ])
  kql_databases = local.rti_enabled ? {
    operations = {
      definition = local.kql_database_definition
    }
  } : {}

  activator_candidates = {
    operations = {
      display_name    = "pc_operations_activator"
      description     = "People Counter operational event rules"
      definition_root = "${local.fabric_content_root}/activators/operations"
    }
    manifest_arrival = {
      display_name    = "pc_manifest_arrival_activator"
      description     = "People Counter manifest arrival event rules"
      definition_root = "${local.fabric_content_root}/activators/manifest-arrival"
    }
  }
  activators = var.enable_activator ? {
    for key, activator in local.activator_candidates :
    key => merge(activator, {
      metadata = jsondecode(file("${activator.definition_root}/.fabric-item.json"))
    })
    if fileexists("${activator.definition_root}/.fabric-item.json")
  } : {}

  managed_tag_ids = toset([
    for tag in fabric_tag.this : tag.id
  ])
  item_tags = setunion(var.tags, local.managed_tag_ids)
}

resource "fabric_workspace" "this" {
  display_name                   = var.workspace_display_name
  description                    = var.workspace_description
  capacity_id                    = var.capacity_id
  skip_capacity_state_validation = var.skip_capacity_state_validation

  identity = var.enable_workspace_identity ? {
    type = "SystemAssigned"
  } : null

  lifecycle {
    prevent_destroy = true
  }
}

resource "fabric_workspace_role_assignment" "this" {
  for_each = var.workspace_role_assignments

  workspace_id = fabric_workspace.this.id
  principal = {
    id   = each.value.principal_id
    type = each.value.principal_type
  }
  role = each.value.role
}

resource "fabric_folder" "this" {
  for_each = var.workspace_folders

  display_name = each.value.display_name
  workspace_id = fabric_workspace.this.id

  lifecycle {
    precondition {
      condition     = var.enable_preview_features
      error_message = "workspace_folders require enable_preview_features=true and provider preview mode."
    }
  }
}

resource "fabric_domain" "this" {
  count = var.domain == null ? 0 : 1

  display_name     = var.domain.display_name
  description      = var.domain.description
  default_label_id = var.domain.default_label_id

  lifecycle {
    prevent_destroy = true
  }
}

resource "fabric_domain_role_assignments" "this" {
  for_each = var.domain == null ? {} : var.domain.role_assignments

  domain_id  = fabric_domain.this[0].id
  role       = each.value.role
  principals = each.value.principals
}

resource "fabric_domain_workspace_assignments" "this" {
  count = var.domain == null ? 0 : (var.domain.assign_workspace ? 1 : 0)

  domain_id     = fabric_domain.this[0].id
  workspace_ids = [fabric_workspace.this.id]

  lifecycle {
    precondition {
      condition     = var.enable_preview_features
      error_message = "Domain workspace assignment requires enable_preview_features=true and provider preview mode."
    }
  }
}

resource "fabric_tag" "this" {
  for_each = var.governance_tags

  display_name = each.value.display_name
  scope = {
    type = each.value.scope
    domain_id = (
      each.value.scope == "Domain"
      ? fabric_domain.this[0].id
      : null
    )
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "fabric_connection" "this" {
  for_each = var.non_secret_connections

  display_name      = each.value.display_name
  connectivity_type = each.value.connectivity_type
  gateway_id        = each.value.gateway_id
  privacy_level     = each.value.privacy_level
  allow_connection_usage_in_gateway = (
    each.value.connectivity_type == "VirtualNetworkGateway"
    ? null
    : each.value.allow_connection_usage_in_gateway
  )
  allow_usage_in_user_controlled_code = (
    each.value.connectivity_type == "VirtualNetworkGateway"
    ? null
    : each.value.allow_usage_in_user_controlled_code
  )

  connection_details = {
    type            = each.value.connection_type
    creation_method = each.value.creation_method
    parameters = length(each.value.parameters) == 0 ? null : [
      for name, value in each.value.parameters : {
        name  = name
        value = value
      }
    ]
  }

  credential_details = {
    credential_type       = each.value.credential_type
    connection_encryption = each.value.connection_encryption
    single_sign_on_type   = each.value.single_sign_on_type
    skip_test_connection  = each.value.skip_test_connection
  }

  lifecycle {
    precondition {
      condition = (
        each.value.credential_type != "WorkspaceIdentity"
        || var.enable_workspace_identity
      )
      error_message = "WorkspaceIdentity connections require enable_workspace_identity=true."
    }
  }
}

resource "fabric_connection_role_assignment" "this" {
  for_each = local.connection_role_assignments

  connection_id = fabric_connection.this[each.value.connection_key].id
  principal = {
    id   = each.value.principal_id
    type = each.value.principal_type
  }
  role = each.value.role
}

resource "fabric_lakehouse" "this" {
  display_name = var.lakehouse_display_name
  description  = var.lakehouse_description
  workspace_id = fabric_workspace.this.id
  tags         = local.item_tags

  configuration = {
    enable_schemas = true
  }

  lifecycle {
    prevent_destroy = true
  }
}

data "azurerm_storage_account" "adls" {
  name                = local.adls_storage_account_name
  resource_group_name = split("/", var.adls_storage_account_resource_id)[4]

  lifecycle {
    postcondition {
      condition     = self.is_hns_enabled
      error_message = "The ADLS shortcut storage account must have hierarchical namespace enabled."
    }
  }
}

resource "azurerm_role_assignment" "adls_workspace_identity" {
  scope                = var.adls_storage_account_resource_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = fabric_workspace.this.identity.service_principal_id

  lifecycle {
    precondition {
      condition     = var.enable_workspace_identity
      error_message = "The ADLS shortcut requires workspace identity."
    }
  }

  depends_on = [data.azurerm_storage_account.adls]
}

resource "fabric_connection" "adls_workspace_identity" {
  display_name      = "${var.workspace_display_name}-adls"
  connectivity_type = "ShareableCloud"
  privacy_level     = "Organizational"

  connection_details = {
    type            = "AzureDataLakeStorage"
    creation_method = "AzureDataLakeStorage"
    parameters = [{
      name  = "server"
      value = local.adls_location
    }]
  }

  credential_details = {
    credential_type       = "WorkspaceIdentity"
    connection_encryption = "NotEncrypted"
    single_sign_on_type   = "None"
    skip_test_connection  = true
  }

  depends_on = [azurerm_role_assignment.adls_workspace_identity]
}

resource "fabric_shortcut" "adls" {
  workspace_id = fabric_workspace.this.id
  item_id      = fabric_lakehouse.this.id
  name         = var.adls_shortcut_name
  path         = var.adls_shortcut_path

  target = {
    adls_gen2 = {
      location      = local.adls_location
      subpath       = local.adls_subpath
      connection_id = fabric_connection.adls_workspace_identity.id
    }
  }

  shortcut_conflict_policy = "Abort"
}

resource "fabric_environment" "this" {
  display_name = var.environment_display_name
  description  = var.environment_description
  workspace_id = fabric_workspace.this.id
  tags         = local.item_tags

  lifecycle {
    prevent_destroy = true
  }
}

resource "fabric_spark_job_definition" "this" {
  for_each = var.spark_job_definitions

  display_name = each.value.display_name
  description  = each.value.description
  workspace_id = fabric_workspace.this.id
  format       = "SparkJobDefinitionV2"
  tags         = local.item_tags

  definition = {
    "Main/main.py" = {
      source = "${local.spark_job_definitions_root}/${each.key}/Main/main.py"
    }

    "SparkJobDefinitionV1.json" = {
      source          = "${local.spark_job_definitions_root}/common/SparkJobDefinitionV1.json.tmpl"
      processing_mode = "GoTemplate"
      tokens = {
        DefaultLakehouseArtifactId = fabric_lakehouse.this.id
        EnvironmentArtifactId      = fabric_environment.this.id
      }
    }
  }

  depends_on = [
    fabric_lakehouse.this,
    fabric_environment.this,
  ]
}

resource "fabric_variable_library" "deployment" {
  count = var.variable_library == null ? 0 : 1

  display_name = var.variable_library.display_name
  description  = var.variable_library.description
  workspace_id = fabric_workspace.this.id
  folder_id = (
    var.variable_library.folder_key == null
    ? null
    : fabric_folder.this[var.variable_library.folder_key].id
  )
  format                    = "Default"
  definition_update_enabled = true
  tags                      = local.item_tags

  definition = {
    "settings.json" = {
      source          = "${local.variable_library_root}/settings.json"
      processing_mode = "None"
    }
    "variables.json" = {
      source          = "${local.variable_library_root}/variables.json.tmpl"
      processing_mode = "GoTemplate"
      tokens = merge(
        {
          DeploymentStage = var.variable_library.deployment_stage
          WorkspaceId     = fabric_workspace.this.id
          LakehouseId     = fabric_lakehouse.this.id
          EnvironmentId   = fabric_environment.this.id
        },
        {
          for role, job in fabric_spark_job_definition.this :
          "SparkJob${title(role)}Id" => job.id
        },
      )
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "fabric_onelake_data_access_security" "this" {
  for_each = var.onelake_data_access_roles

  workspace_id   = fabric_workspace.this.id
  item_id        = fabric_lakehouse.this.id
  role_name      = each.value.role_name
  members        = each.value.members
  decision_rules = each.value.decision_rules

  lifecycle {
    precondition {
      condition     = var.enable_preview_features
      error_message = "OneLake data access roles require enable_preview_features=true and provider preview mode."
    }
  }
}

resource "fabric_semantic_model" "this" {
  for_each = local.semantic_models

  display_name = each.value.display_name
  description  = each.value.description
  workspace_id = fabric_workspace.this.id
  format       = each.value.metadata.format
  tags         = local.item_tags

  definition = {
    for part in each.value.metadata.parts :
    part.path => {
      source          = "${each.value.definition_root}/${part.path}"
      processing_mode = "GoTemplate"
      tokens = {
        TargetWorkspaceId   = fabric_workspace.this.id
        TargetLakehouseId   = fabric_lakehouse.this.id
        TargetLakehouseName = fabric_lakehouse.this.display_name
      }
    }
    if part.path != ".platform"
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "fabric_report" "this" {
  for_each = local.reports

  display_name = each.value.display_name
  description  = each.value.description
  workspace_id = fabric_workspace.this.id
  format       = each.value.metadata.format
  tags         = local.item_tags

  definition = {
    for part in each.value.metadata.parts :
    part.path => {
      source          = "${each.value.definition_root}/${part.path}"
      processing_mode = "GoTemplate"
      tokens = {
        TargetSemanticModelId   = fabric_semantic_model.this[each.key].id
        TargetSemanticModelName = fabric_semantic_model.this[each.key].display_name
      }
    }
    if part.path != ".platform"
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "fabric_eventhouse" "operations" {
  count = local.rti_enabled ? 1 : 0

  display_name = "pc_operations_eventhouse"
  description  = "People Counter operational telemetry Eventhouse"
  workspace_id = fabric_workspace.this.id
  format = (
    local.eventhouse_definition == null
    ? null
    : local.eventhouse_definition.metadata.format
  )
  tags = local.item_tags

  definition = (
    local.eventhouse_definition == null
    ? null
    : {
      for part in local.eventhouse_definition.metadata.parts :
      part.path => {
        source = (
          "${local.eventhouse_definition.definition_root}/${part.path}"
        )
        processing_mode = "GoTemplate"
        tokens = {
          TargetWorkspaceId = fabric_workspace.this.id
        }
      }
    }
  )

  configuration = {
    minimum_consumption_units = 0
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "fabric_kql_database" "operations" {
  for_each = local.kql_databases

  display_name = "pc_operations"
  description  = "People Counter operational telemetry database"
  workspace_id = fabric_workspace.this.id
  format = (
    each.value.definition == null
    ? null
    : each.value.definition.metadata.format
  )
  tags = local.item_tags

  configuration = {
    database_type = "ReadWrite"
    eventhouse_id = fabric_eventhouse.operations[0].id
  }

  definition = (
    each.value.definition == null
    ? null
    : {
      for part in each.value.definition.metadata.parts :
      part.path => {
        source          = "${each.value.definition.definition_root}/${part.path}"
        processing_mode = "GoTemplate"
        tokens = {
          TargetWorkspaceId  = fabric_workspace.this.id
          TargetEventhouseId = fabric_eventhouse.operations[0].id
        }
      }
    }
  )

  lifecycle {
    prevent_destroy = true
  }
}

resource "fabric_kql_queryset" "operations" {
  for_each = local.kql_querysets

  display_name = "pc_operations_queryset"
  description  = "People Counter operational telemetry queries"
  workspace_id = fabric_workspace.this.id
  format       = each.value.metadata.format
  tags         = local.item_tags

  definition = {
    for part in each.value.metadata.parts :
    part.path => {
      source          = "${each.value.definition_root}/${part.path}"
      processing_mode = "GoTemplate"
      tokens = {
        TargetWorkspaceId   = fabric_workspace.this.id
        TargetKqlDatabaseId = fabric_kql_database.operations["operations"].id
        TargetKqlQueryServiceUri = (
          fabric_kql_database.operations["operations"].properties.query_service_uri
        )
      }
    }
    if part.path != ".platform"
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "fabric_kql_dashboard" "operations" {
  for_each = local.kql_dashboards

  display_name = "pc_operations_dashboard"
  description  = "People Counter workspace and runtime operations dashboard"
  workspace_id = fabric_workspace.this.id
  format       = each.value.metadata.format
  tags         = local.item_tags

  definition = {
    for part in each.value.metadata.parts :
    part.path => {
      source          = "${each.value.definition_root}/${part.path}"
      processing_mode = "GoTemplate"
      tokens = {
        TargetWorkspaceId   = fabric_workspace.this.id
        TargetKqlDatabaseId = fabric_kql_database.operations["operations"].id
        TargetKqlQueryServiceUri = (
          fabric_kql_database.operations["operations"].properties.query_service_uri
        )
      }
    }
    if part.path != ".platform"
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "fabric_activator" "this" {
  for_each = local.activators

  display_name              = each.value.display_name
  description               = each.value.description
  workspace_id              = fabric_workspace.this.id
  format                    = each.value.metadata.format
  definition_update_enabled = true
  tags                      = local.item_tags

  definition = {
    for part in each.value.metadata.parts :
    part.path => {
      source          = "${each.value.definition_root}/${part.path}"
      processing_mode = "GoTemplate"
      tokens = merge(
        {
          TargetWorkspaceId = fabric_workspace.this.id
        },
        local.rti_enabled ? {
          TargetEventhouseId = fabric_eventhouse.operations[0].id
        } : {},
        local.rti_enabled ? {
          TargetKqlDatabaseId = fabric_kql_database.operations["operations"].id
        } : {},
        local.kql_queryset_definition == null ? {} : {
          TargetKqlQuerysetId = fabric_kql_queryset.operations["operations"].id
        },
        local.kql_dashboard_definition == null ? {} : {
          TargetKqlDashboardId = fabric_kql_dashboard.operations["operations"].id
        }
      )
    }
    if part.path != ".platform"
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "fabric_deployment_pipeline" "this" {
  count = local.deployment_pipeline_enabled ? 1 : 0

  display_name = var.deployment_pipeline.display_name
  description  = var.deployment_pipeline.description

  stages = [
    for index, stage in var.deployment_pipeline.stages : {
      display_name = stage.display_name
      description  = stage.description
      is_public    = stage.is_public
      workspace_id = index == 0 ? fabric_workspace.this.id : null
    }
  ]

  lifecycle {
    prevent_destroy = true
  }
}

resource "fabric_deployment_pipeline_role_assignment" "this" {
  for_each = var.deployment_pipeline == null ? {} : var.deployment_pipeline.role_assignments

  deployment_pipeline_id = fabric_deployment_pipeline.this[0].id
  principal = {
    id   = each.value.principal_id
    type = each.value.principal_type
  }
  role = each.value.role
}
