# Proof-of-concept Infrastructure-as-Code that provisions a managed Kubernetes
# (EKS) control plane to host the distributed-api-layer telemetry service.
# Values are illustrative mocks intended to demonstrate IaC structure.

terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  description = "AWS region used to host the cluster."
  type        = string
  default     = "eu-west-1"
}

variable "cluster_name" {
  description = "Name of the managed Kubernetes cluster."
  type        = string
  default     = "distributed-api-layer-cluster"
}

variable "cluster_version" {
  description = "Kubernetes control-plane version."
  type        = string
  default     = "1.30"
}

variable "subnet_ids" {
  description = "Subnets the cluster control plane is attached to."
  type        = list(string)
  default     = ["subnet-abc12345", "subnet-def67890"]
}

resource "aws_eks_cluster" "distributed_api_layer" {
  name     = var.cluster_name
  role_arn = "arn:aws:iam::123456789012:role/EKSClusterRole"
  version  = var.cluster_version

  vpc_config {
    subnet_ids = var.subnet_ids
  }

  tags = {
    Project     = "distributed-core-systems"
    Component   = "distributed-api-layer"
    ManagedBy   = "terraform"
    Environment = "production"
  }
}

output "cluster_name" {
  description = "Name of the provisioned cluster."
  value       = aws_eks_cluster.distributed_api_layer.name
}

output "cluster_endpoint" {
  description = "API server endpoint for the provisioned cluster."
  value       = aws_eks_cluster.distributed_api_layer.endpoint
}
