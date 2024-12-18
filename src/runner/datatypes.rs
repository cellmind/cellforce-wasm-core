use arrow_schema::DataType;

pub fn udf_type_to_arrow_type(udf_type: &str) -> arrow::datatypes::DataType {
    match udf_type {
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "float32" => DataType::Float16,
        "float64" => DataType::Float64,
        "string" => DataType::LargeUtf8,
        "date32" => DataType::Date32,
        "date64" => DataType::Date64,
        "boolean" => DataType::Boolean,
        _ => panic!("unsupported udf type: {}", udf_type),
    }
}

