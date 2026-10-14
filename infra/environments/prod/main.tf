locals {
  spark_job_definitions = {
    control        = { display_name = "pc-sjd-control" }
    dispatcher     = { display_name = "pc-sjd-dispatcher" }
    process        = { display_name = "pc-sjd-process" }
    refresh        = { display_name = "pc-sjd-refresh" }
    reconciliation = { display_name = "pc-sjd-reconciliation" }
    gold           = { display_name = "pc-sjd-gold" }
    cutover        = { display_name = "pc-sjd-cutover" }
  }
  reporting_items = {
    analytics = {
      semantic_model_display_name = "pc_sjd_analytics_model"
      report_display_name         = "pc_sjd_analytics_report"
    }
    operational = {
      semantic_model_display_name = "pc_sjd_operations_model"
      report_display_name         = "pc_sjd_operations_report"
    }
  }
}

module "fabric_foundation" {
  source = "../../modules/fabric-foundation"

  workspace_display_name = "people-counter-prod"
  workspace_description  = "People Counter production workspace"
  capacity_id            = var.capacity_id

  workspace_role_assignments = var.workspace_role_assignments

  lakehouse_display_name = "pc_prod_lakehouse"
  lakehouse_description  = "People Counter production Lakehouse"

  environment_display_name = "pc_prod_environment"
  environment_description  = "People Counter production Spark Environment"

  spark_job_definitions            = local.spark_job_definitions
  reporting_items                  = local.reporting_items
  enable_activator                 = var.enable_activator
  adls_storage_account_resource_id = var.adls_storage_account_resource_id
  adls_shortcut_name               = var.adls_shortcut_name
  adls_shortcut_path               = var.adls_shortcut_path
  adls_shortcut_container          = var.adls_shortcut_container
  adls_shortcut_target_path        = var.adls_shortcut_target_path
  workspace_folders                = var.workspace_folders
  non_secret_connections           = var.non_secret_connections
  domain                           = var.domain
  governance_tags                  = var.governance_tags
  onelake_data_access_roles        = var.onelake_data_access_roles

  variable_library = {
    display_name     = "pc_prod_variables"
    description      = "People Counter production deployment references"
    deployment_stage = "prod"
  }
}
