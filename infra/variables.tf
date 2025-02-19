variable "stage" {
    description = "The stage of the environment"
    type        = string
    default = "dev"
}

variable "database_name" {
    description = "The name of the database"
    type        = string
    default = "gastos_deputados"
}

variable "bucket_name" {
    description = "The name of the bucket"
    type        = string
    default = "gastos-deputados-9723"
}
