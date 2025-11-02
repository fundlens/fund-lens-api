# Data source to read shared infrastructure remote state
data "terraform_remote_state" "shared" {
  backend = "azurerm"

  config = {
    storage_account_name = var.shared_infra_storage_account
    container_name       = var.shared_infra_container
    key                  = var.shared_infra_key
    resource_group_name  = var.shared_infra_resource_group
  }
}

# Container Apps Environment
resource "azurerm_container_app_environment" "main" {
  name                           = "${var.project_name}-${var.environment}-env"
  location                       = data.terraform_remote_state.shared.outputs.location
  resource_group_name            = data.terraform_remote_state.shared.outputs.resource_group_name
  log_analytics_workspace_id     = data.terraform_remote_state.shared.outputs.log_analytics_workspace_id

  infrastructure_subnet_id       = data.terraform_remote_state.shared.outputs.container_apps_subnet_id
  internal_load_balancer_enabled = false
}

# Container App
resource "azurerm_container_app" "main" {
  name                         = "${var.project_name}-api-${var.environment}-app"
  container_app_environment_id = azurerm_container_app_environment.main.id
  resource_group_name          = data.terraform_remote_state.shared.outputs.resource_group_name
  revision_mode                = "Single"

  # Registry credentials from shared infrastructure
  secret {
    name  = "registry-password"
    value = data.terraform_remote_state.shared.outputs.acr_admin_password
  }

  registry {
    server               = data.terraform_remote_state.shared.outputs.acr_login_server
    username             = data.terraform_remote_state.shared.outputs.acr_admin_username
    password_secret_name = "registry-password"
  }

  template {
    container {
      name   = "${var.project_name}-api"
      image  = "${data.terraform_remote_state.shared.outputs.acr_name}.azurecr.io/${var.container_image_tag}"
      cpu    = var.cpu
      memory = var.memory

      # Dynamic environment variables from app_env_vars
      dynamic "env" {
        for_each = var.app_env_vars
        content {
          name  = env.key
          value = env.value
        }
      }
    }

    min_replicas = var.min_replicas
    max_replicas = var.max_replicas
  }

  ingress {
    external_enabled = true
    target_port      = var.container_port
    transport        = "auto"

    traffic_weight {
      latest_revision = true
      percentage      = 100
    }
  }
}