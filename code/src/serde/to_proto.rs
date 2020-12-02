use std::convert::TryInto;

use crate::datasource::ParquetTable;
use crate::protobuf;
use arrow::datatypes::{DataType, Schema};
use datafusion::logical_plan::{Expr, LogicalPlan, TableSource};
use datafusion::physical_plan::aggregates;
use datafusion::scalar::ScalarValue;

use snafu::{OptionExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DataFusion error"))]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },
    #[snafu(display("Deser error: {}", msg))]
    Str { msg: String },
    #[snafu(display("Not implemented: {}", msg))]
    NotImplemented { msg: String },
}

impl TryInto<protobuf::Schema> for &Schema {
    type Error = Error;

    fn try_into(self) -> Result<protobuf::Schema, Self::Error> {
        Ok(protobuf::Schema {
            columns: self
                .fields()
                .iter()
                .map(|field| {
                    let proto = to_proto_arrow_type(&field.data_type());
                    proto.and_then(|arrow_type| {
                        Ok(protobuf::Field {
                            name: field.name().to_owned(),
                            arrow_type: arrow_type.into(),
                            nullable: field.is_nullable(),
                            children: vec![],
                        })
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

fn to_proto_arrow_type(dt: &DataType) -> Result<protobuf::ArrowType, Error> {
    match dt {
        DataType::Int8 => Ok(protobuf::ArrowType::Int8),
        DataType::Int16 => Ok(protobuf::ArrowType::Int16),
        DataType::Int32 => Ok(protobuf::ArrowType::Int32),
        DataType::Int64 => Ok(protobuf::ArrowType::Int64),
        DataType::UInt8 => Ok(protobuf::ArrowType::Uint8),
        DataType::UInt16 => Ok(protobuf::ArrowType::Uint16),
        DataType::UInt32 => Ok(protobuf::ArrowType::Uint32),
        DataType::UInt64 => Ok(protobuf::ArrowType::Uint64),
        DataType::Float32 => Ok(protobuf::ArrowType::Float),
        DataType::Float64 => Ok(protobuf::ArrowType::Double),
        DataType::Utf8 => Ok(protobuf::ArrowType::Utf8),
        other => Err(Error::Str {
            msg: format!("Unsupported data type {:?}", other),
        }),
    }
}

impl TryInto<protobuf::LogicalPlanNode> for &LogicalPlan {
    type Error = Error;

    fn try_into(self) -> Result<protobuf::LogicalPlanNode, Self::Error> {
        match self {
            LogicalPlan::TableScan {
                source: TableSource::FromProvider(provider),
                table_schema,
                projection,
                ..
            } => {
                let mut node = empty_logical_plan_node();

                let projection = projection.as_ref().map(|column_indices| {
                    let columns: Vec<String> = column_indices
                        .iter()
                        .map(|i| table_schema.field(*i).name().clone())
                        .collect();
                    protobuf::ProjectionColumns { columns }
                });

                let schema: protobuf::Schema = table_schema.as_ref().try_into()?;

                // Only parquet s3 tables supported here
                let parquet_table = provider
                    .as_any()
                    .downcast_ref::<ParquetTable>()
                    .context(NotImplemented {
                        msg: format!("table source to_proto {:?}", self),
                    })?;

                node.scan = Some(protobuf::S3ParquetScanNode {
                    region: parquet_table.region().to_owned(),
                    bucket: parquet_table.bucket().to_owned(),
                    files: parquet_table
                        .files()
                        .iter()
                        .map(|sized_file| protobuf::SizedFile {
                            key: sized_file.key.to_owned(),
                            length: sized_file.length,
                        })
                        .collect(),
                    projection,
                    schema: Some(schema),
                });

                Ok(node)
            }
            LogicalPlan::Projection { expr, input, .. } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                let mut node = empty_logical_plan_node();
                node.input = Some(Box::new(input));
                node.projection = Some(protobuf::ProjectionNode {
                    expr: expr
                        .iter()
                        .map(|expr| expr.try_into())
                        .collect::<Result<Vec<_>, Error>>()?,
                });
                Ok(node)
            }
            LogicalPlan::Filter { predicate, input } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                let mut node = empty_logical_plan_node();
                node.input = Some(Box::new(input));
                node.selection = Some(protobuf::SelectionNode {
                    expr: Some(predicate.try_into()?),
                });
                Ok(node)
            }
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                ..
            } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                let mut node = empty_logical_plan_node();
                node.input = Some(Box::new(input));
                node.aggregate = Some(protobuf::AggregateNode {
                    group_expr: group_expr
                        .iter()
                        .map(|expr| expr.try_into())
                        .collect::<Result<Vec<_>, Error>>()?,
                    aggr_expr: aggr_expr
                        .iter()
                        .map(|expr| expr.try_into())
                        .collect::<Result<Vec<_>, Error>>()?,
                });
                Ok(node)
            }
            _ => Err(Error::NotImplemented {
                msg: format!("logical plan to_proto {:?}", self),
            }),
        }
    }
}

impl TryInto<protobuf::LogicalExprNode> for &Expr {
    type Error = Error;

    fn try_into(self) -> Result<protobuf::LogicalExprNode, Self::Error> {
        match self {
            Expr::Column(name) => {
                let mut expr = empty_expr_node();
                expr.has_column_name = true;
                expr.column_name = name.clone();
                Ok(expr)
            }
            Expr::Alias(expr, alias) => {
                let mut expr_node = empty_expr_node();
                expr_node.alias = Some(Box::new(protobuf::AliasNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    alias: alias.to_owned(),
                }));
                Ok(expr_node)
            }
            Expr::Literal(value) => match value {
                ScalarValue::Utf8(s) => {
                    let mut expr = empty_expr_node();
                    if let Some(s) = s {
                        expr.has_literal_string = true;
                        expr.literal_string = s.to_owned();
                    }
                    Ok(expr)
                }
                ScalarValue::Int8(n) => {
                    let mut expr = empty_expr_node();
                    if let Some(n) = n {
                        expr.has_literal_i8 = true;
                        expr.literal_int = *n as i64;
                    }
                    Ok(expr)
                }
                ScalarValue::Int16(n) => {
                    let mut expr = empty_expr_node();
                    if let Some(n) = n {
                        expr.has_literal_i16 = true;
                        expr.literal_int = *n as i64;
                    }
                    Ok(expr)
                }
                ScalarValue::Int32(n) => {
                    let mut expr = empty_expr_node();
                    if let Some(n) = n {
                        expr.has_literal_i32 = true;
                        expr.literal_int = *n as i64;
                    }
                    Ok(expr)
                }
                ScalarValue::Int64(n) => {
                    let mut expr = empty_expr_node();
                    if let Some(n) = n {
                        expr.has_literal_i64 = true;
                        expr.literal_int = *n as i64;
                    }
                    Ok(expr)
                }
                ScalarValue::UInt8(n) => {
                    let mut expr = empty_expr_node();
                    if let Some(n) = n {
                        expr.has_literal_u8 = true;
                        expr.literal_uint = *n as u64;
                    }
                    Ok(expr)
                }
                ScalarValue::UInt16(n) => {
                    let mut expr = empty_expr_node();
                    if let Some(n) = n {
                        expr.has_literal_u16 = true;
                        expr.literal_uint = *n as u64;
                    }
                    Ok(expr)
                }
                ScalarValue::UInt32(n) => {
                    let mut expr = empty_expr_node();
                    if let Some(n) = n {
                        expr.has_literal_u32 = true;
                        expr.literal_uint = *n as u64;
                    }
                    Ok(expr)
                }
                ScalarValue::UInt64(n) => {
                    let mut expr = empty_expr_node();
                    if let Some(n) = n {
                        expr.has_literal_u64 = true;
                        expr.literal_uint = *n as u64;
                    }
                    Ok(expr)
                }
                ScalarValue::Float32(n) => {
                    let mut expr = empty_expr_node();
                    if let Some(n) = n {
                        expr.has_literal_f32 = true;
                        expr.literal_f32 = *n;
                    }
                    Ok(expr)
                }
                ScalarValue::Float64(n) => {
                    let mut expr = empty_expr_node();
                    if let Some(n) = n {
                        expr.has_literal_f64 = true;
                        expr.literal_f64 = *n;
                    }
                    Ok(expr)
                }
                other => Err(Error::NotImplemented {
                    msg: format!("to_proto unsupported scalar value {:?}", other),
                }),
            },
            Expr::BinaryExpr { left, op, right } => {
                let mut expr = empty_expr_node();
                expr.binary_expr = Some(Box::new(protobuf::BinaryExprNode {
                    l: Some(Box::new(left.as_ref().try_into()?)),
                    r: Some(Box::new(right.as_ref().try_into()?)),
                    op: format!("{:?}", op),
                }));
                Ok(expr)
            }
            Expr::AggregateFunction { fun, ref args, .. } => {
                let mut expr = empty_expr_node();

                let aggr_function = match fun {
                    aggregates::AggregateFunction::Min => {
                        Ok(protobuf::AggregateFunction::Min)
                    }
                    aggregates::AggregateFunction::Max => {
                        Ok(protobuf::AggregateFunction::Max)
                    }
                    aggregates::AggregateFunction::Sum => {
                        Ok(protobuf::AggregateFunction::Sum)
                    }
                    aggregates::AggregateFunction::Avg => {
                        Ok(protobuf::AggregateFunction::Avg)
                    }
                    aggregates::AggregateFunction::Count => {
                        Ok(protobuf::AggregateFunction::Count)
                    }
                }?;

                let arg = &args[0];
                expr.aggregate_expr = Some(Box::new(protobuf::AggregateExprNode {
                    aggr_function: aggr_function.into(),
                    expr: Some(Box::new(arg.try_into()?)),
                }));
                Ok(expr)
            }
            _ => Err(Error::NotImplemented {
                msg: format!("logical expr to_proto {:?}", self),
            }),
        }
    }
}

/// Create an empty ExprNode
fn empty_expr_node() -> protobuf::LogicalExprNode {
    protobuf::LogicalExprNode {
        alias: None,
        column_name: "".to_owned(),
        has_column_name: false,
        literal_string: "".to_owned(),
        has_literal_string: false,
        literal_int: 0,
        literal_uint: 0,
        literal_f32: 0.0,
        literal_f64: 0.0,
        has_literal_i8: false,
        has_literal_i16: false,
        has_literal_i32: false,
        has_literal_i64: false,
        has_literal_u8: false,
        has_literal_u16: false,
        has_literal_u32: false,
        has_literal_u64: false,
        has_literal_f32: false,
        has_literal_f64: false,
        binary_expr: None,
        aggregate_expr: None,
    }
}

/// Create an empty LogicalPlanNode
fn empty_logical_plan_node() -> protobuf::LogicalPlanNode {
    protobuf::LogicalPlanNode {
        scan: None,
        input: None,
        projection: None,
        selection: None,
        limit: None,
        aggregate: None,
    }
}
