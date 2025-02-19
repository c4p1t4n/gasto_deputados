resource "aws_athena_database" "this" {
    name   = "${var.database_name}_${var.stage}"
    bucket = aws_s3_bucket.main_bucket-dev.bucket
}


resource "aws_glue_catalog_table" "deputados" {
    name          = "deputados_${var.stage}"
    database_name = aws_athena_database.this.name
    table_type = "EXTERNAL_TABLE"

    parameters = {
        EXTERNAL              = "TRUE"
        "parquet.compression" = "SNAPPY"
    }

    storage_descriptor {
        location      = "s3://${aws_s3_bucket.main_bucket-dev.bucket}/deputados/"
        input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
        output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
        name                  = "my-stream"
        serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

        parameters = {
            "serialization.format" = 1
        }
    }

    columns {
        name = "id"
        type = "int"
    }

    columns {
        name = "uri"
        type = "string"
    }

    columns {
        name = "nome"
        type = "string"
    }

    columns {
        name = "siglaPartido"
        type = "string"
    }

    columns {
        name = "uriPartido"
        type = "string"
    }

    columns {
        name = "siglaUf"
        type = "string"
    }

    columns {
        name = "idLegislatura"
        type = "int"
    }

    columns {
        name = "urlFoto"
        type = "string"
    }
}
}



resource "aws_glue_catalog_table" "gastos" {
    name          = "gastos_${var.stage}"
    database_name = aws_athena_database.this.name
    table_type    = "EXTERNAL_TABLE"

    parameters = {
        EXTERNAL              = "TRUE"
        "parquet.compression" = "SNAPPY"
    }

    storage_descriptor {
        location      = "s3://${aws_s3_bucket.main_bucket-dev.bucket}/processed/gastos/"
        input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
        output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

        ser_de_info {
            name                  = "deputado-serde"
            serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

            parameters = {
                "serialization.format" = 1
            }
        }
    

    columns {
        name = "id"
        type = "int"
    }

    columns {
        name = "ano"
        type = "int"
    }

    columns {
        name = "mes"
        type = "int"
    }

    columns {
        name = "tipoDespesa"
        type = "string"
    }

    columns {
        name = "codDocumento"
        type = "int"
    }

    columns {
        name = "tipoDocumento"
        type = "string"
    }

    columns {
        name = "codTipoDocumento"
        type = "int"
    }

    columns {
        name = "dataDocumento"
        type = "timestamp"
    }

    columns {
        name = "numDocumento"
        type = "string"
    }

    columns {
        name = "valorDocumento"
        type = "double"
    }

    columns {
        name = "urlDocumento"
        type = "string"
    }

    columns {
        name = "nomeFornecedor"
        type = "string"
    }

    columns {
        name = "cnpjCpfFornecedor"
        type = "string"
    }

    columns {
        name = "valorLiquido"
        type = "double"
    }

    columns {
        name = "valorGlosa"
        type = "double"
    }

    columns {
        name = "numRessarcimento"
        type = "string"
    }

    columns {
        name = "codLote"
        type = "int"
    }

    columns {
        name = "parcela"
        type = "int"
    }
    }
}
