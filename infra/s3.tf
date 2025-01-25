resource "aws_s3_bucket" "main_bucket-prod" {
    bucket = "gastos-deputados-9723-prod"
    tags = {
        project="gasto-deputados"
        env = "prod"
    }
}

resource "aws_s3_bucket" "main_bucket-dev" {
    bucket = "gastos-deputados-9723-dev"
    tags = {
        project="gasto-deputados"
        env = "dev"
    }
}