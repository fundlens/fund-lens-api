# Remote State Configuration
variable "shared_infra_storage_account" {
  description = "Storage account name for shared infrastructure remote state"
  type        = string
}

variable "shared_infra_container" {
  description = "Container name for shared infrastructure remote state"
  type        = string
}

variable "shared_infra_key" {
  description = "Blob name (key) for shared infrastructure remote state"
  type        = string
}

variable "shared_infra_resource_group" {
  description = "Resource group containing the shared infrastructure storage account"
  type        = string
}

# Project Configuration
variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "fund-lens-api"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "prod"
}

# Container Configuration
variable "container_image_tag" {
  description = "Full container image path including tag"
  type        = string
}

variable "container_port" {
  description = "Port the container listens on"
  type        = number
  default     = 8000
}

# Container Resources
variable "cpu" {
  description = "CPU allocation for the container (in cores)"
  type        = number
  default     = 0.5
}

variable "memory" {
  description = "Memory allocation for the container"
  type        = string
  default     = "1Gi"
}

# Scaling Configuration
variable "min_replicas" {
  description = "Minimum number of container replicas"
  type        = number
  default     = 1
}

variable "max_replicas" {
  description = "Maximum number of container replicas"
  type        = number
  default     = 1
}

# Application Environment Variables
variable "app_env_vars" {
  description = "Environment variables for the FastAPI application"
  type        = map(string)
  default     = {}
  sensitive   = true
}
