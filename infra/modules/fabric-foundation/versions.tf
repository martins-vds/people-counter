terraform {
  required_version = ">= 1.9.0, < 2.0.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 5.0"
    }
    fabric = {
      source  = "microsoft/fabric"
      version = "~> 1.14"
    }
  }
}
