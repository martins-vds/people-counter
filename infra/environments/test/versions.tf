terraform {
  required_version = ">= 1.9.0, < 2.0.0"

  required_providers {
    fabric = {
      source  = "microsoft/fabric"
      version = "~> 1.14"
    }
  }

  backend "azurerm" {}
}

provider "fabric" {
  preview = var.enable_preview_features
}
