locals {
  spark_job_definitions = {
    control        = { display_name = "pc-sjd-dev-control" }
    dispatcher     = { display_name = "pc-sjd-dev-dispatcher" }
    process        = { display_name = "pc-sjd-dev-process" }
    refresh        = { display_name = "pc-sjd-dev-refresh" }
    reconciliation = { display_name = "pc-sjd-dev-reconciliation" }
    gold           = { display_name = "pc-sjd-dev-gold" }
    cutover        = { display_name = "pc-sjd-dev-cutover" }
  }
  reporting_items = {
    analytics = {
      semantic_model_display_name = "pc_sjd_dev_analytics_model"
      report_display_name         = "pc_sjd_dev_analytics_report"
    }
    operational = {
      semantic_model_display_name = "pc_sjd_dev_operations_model"
      report_display_name         = "pc_sjd_dev_operations_report"
    }
  }
}

module "fabric_foundation" {
  source = "../../modules/fabric-foundation"

  workspace_display_name = "people-counter-dev"
  workspace_description  = "People Counter development workspace"
  capacity_id            = var.capacity_id

  workspace_role_assignments = var.workspace_role_assignments

  lakehouse_display_name = "pc_dev_lakehouse"
  lakehouse_description  = "People Counter development Lakehouse"

  environment_display_name = "pc_dev_environment"
  environment_description  = "People Counter development Spark Environment"

  spark_job_definitions     = local.spark_job_definitions
  reporting_items           = local.reporting_items
  deployment_pipeline       = var.deployment_pipeline
  enable_activator          = var.enable_activator
  onelake_shortcuts         = var.onelake_shortcuts
  enable_preview_features   = var.enable_preview_features
  workspace_folders         = var.workspace_folders
  non_secret_connections    = var.non_secret_connections
  domain                    = var.domain
  governance_tags           = var.governance_tags
  onelake_data_access_roles = var.onelake_data_access_roles

  variable_library = {
    display_name     = "pc_dev_variables"
    description      = "People Counter development deployment references"
    deployment_stage = "dev"
  }
}
