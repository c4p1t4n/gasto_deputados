terraform {
    required_version = ">=1.10.3"
    backend "s3" {
        bucket = "terraform-states-9723"
        key = "gastos_deputados/state.tfstate"
        region = "us-east-2"
      
    }
    
    required_providers {
        aws = {
            source  = "hashicorp/aws"
            version = "~> 5.82"
        }
    }

}
provider "aws" {
    region = "us-east-2"
}
