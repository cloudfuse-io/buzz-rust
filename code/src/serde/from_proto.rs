use std::convert::TryInto;
use std::sync::Arc;

use crate::catalog::SizedFile;
use crate::datasource::{ParquetTable, ResultTable};
use crate::error::BuzzError;
use crate::internal_err;
use crate::protobuf;
use arrow::datatypes::Schema;
use arrow::ipc::convert;
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::{
    Expr, LogicalPlan, LogicalPlanBuilder, Operator, ToDFSchema,
};
use datafusion::physical_plan::aggregates;
use datafusion::scalar::ScalarValue;

// macro_rules! convert_required {
//     ($PB:expr) => {{
//         if let Some(field) = $PB.as_ref() {
//             field.try_into()
//         } else {
//             Err(internal_err!("Missing required field in protobuf"))
//         }
//     }};
// }

macro_rules! convert_box_required {
    ($PB:expr) => {{
        if let Some(field) = $PB.as_ref() {
            field.as_ref().try_into()
        } else {
            Err(internal_err!("Missing required field in protobuf"))
        }
    }};
}

impl TryInto<LogicalPlan> for &protobuf::LogicalPlanNode {
    type Error = BuzzError;

    fn try_into(self) -> Result<LogicalPlan, Self::Error> {
        if let Some(projection) = &self.projection {
            let input: LogicalPlan = convert_box_required!(self.input)?;
            LogicalPlanBuilder::from(&input)
                .project(
                    projection
                        .expr
                        .iter()
                        .map(|expr| expr.try_into())
                        .collect::<Result<Vec<_>, _>>()?,
                )?
                .build()
                .map_err(|e| e.into())
        } else if let Some(selection) = &self.selection {
            let input: LogicalPlan = convert_box_required!(self.input)?;
            LogicalPlanBuilder::from(&input)
                .filter(
                    selection
                        .expr
                        .as_ref()
                        .expect("expression required")
                        .try_into()?,
                )?
                .build()
                .map_err(|e| e.into())
        } else if let Some(aggregate) = &self.aggregate {
            let input: LogicalPlan = convert_box_required!(self.input)?;
            let group_expr = aggregate
                .group_expr
                .iter()
                .map(|expr| expr.try_into())
                .collect::<Result<Vec<_>, _>>()?;
            let aggr_expr = aggregate
                .aggr_expr
                .iter()
                .map(|expr| expr.try_into())
                .collect::<Result<Vec<_>, _>>()?;
            LogicalPlanBuilder::from(&input)
                .aggregate(group_expr, aggr_expr)?
                .build()
                .map_err(|e| e.into())
        } else if let Some(scan) = &self.scan {
            // let mut provider: Arc<dyn TableProvider + Send + Sync>;
            // let mut schema: Arc<Schema>;
            let provider: Arc<dyn TableProvider + Send + Sync> = match scan {
                protobuf::logical_plan_node::Scan::S3Parquet(scan_node) => {
                    let schema: Schema = convert::schema_from_bytes(&scan_node.schema)
                        .ok_or_else(|| {
                            internal_err!("Unable to convert flight data to Arrow schema")
                        })?;
                    let provider = ParquetTable::new(
                        scan_node.region.to_owned(),
                        scan_node.bucket.to_owned(),
                        scan_node
                            .files
                            .iter()
                            .map(|sized_file| SizedFile {
                                key: sized_file.key.to_owned(),
                                length: sized_file.length,
                            })
                            .collect(),
                        Arc::new(schema),
                    );
                    Arc::new(provider)
                }
                protobuf::logical_plan_node::Scan::Result(scan_node) => {
                    let schema: Schema = convert::schema_from_bytes(&scan_node.schema)
                        .ok_or_else(|| {
                            internal_err!("Unable to convert flight data to Arrow schema")
                        })?;
                    let provider =
                        ResultTable::new(scan_node.query_id.to_owned(), Arc::new(schema));
                    Arc::new(provider)
                }
            };

            // TODO: projection does not seem to be right
            let projected_schema = provider.schema().to_dfschema_ref()?;
            Ok(LogicalPlan::TableScan {
                table_name: "".to_string(),
                source: provider,
                projected_schema,
                projection: None,
            })
        } else {
            Err(internal_err!("Unsupported logical plan '{:?}'", self))
        }
    }
}

impl TryInto<Expr> for &protobuf::LogicalExprNode {
    type Error = BuzzError;

    fn try_into(self) -> Result<Expr, Self::Error> {
        if let Some(binary_expr) = &self.binary_expr {
            Ok(Expr::BinaryExpr {
                left: Box::new(parse_required_expr(&binary_expr.l)?),
                op: from_proto_binary_op(&binary_expr.op)?,
                right: Box::new(parse_required_expr(&binary_expr.r)?),
            })
        } else if self.has_column_name {
            Ok(Expr::Column(self.column_name.clone()))
        } else if self.has_literal_string {
            Ok(Expr::Literal(ScalarValue::Utf8(Some(
                self.literal_string.clone(),
            ))))
        } else if self.has_literal_f32 {
            Ok(Expr::Literal(ScalarValue::Float32(Some(self.literal_f32))))
        } else if self.has_literal_f64 {
            Ok(Expr::Literal(ScalarValue::Float64(Some(self.literal_f64))))
        } else if self.has_literal_i8 {
            Ok(Expr::Literal(ScalarValue::Int8(Some(
                self.literal_int as i8,
            ))))
        } else if self.has_literal_i16 {
            Ok(Expr::Literal(ScalarValue::Int16(Some(
                self.literal_int as i16,
            ))))
        } else if self.has_literal_i32 {
            Ok(Expr::Literal(ScalarValue::Int32(Some(
                self.literal_int as i32,
            ))))
        } else if self.has_literal_i64 {
            Ok(Expr::Literal(ScalarValue::Int64(Some(
                self.literal_int as i64,
            ))))
        } else if self.has_literal_u8 {
            Ok(Expr::Literal(ScalarValue::UInt8(Some(
                self.literal_uint as u8,
            ))))
        } else if self.has_literal_u16 {
            Ok(Expr::Literal(ScalarValue::UInt16(Some(
                self.literal_uint as u16,
            ))))
        } else if self.has_literal_u32 {
            Ok(Expr::Literal(ScalarValue::UInt32(Some(
                self.literal_uint as u32,
            ))))
        } else if self.has_literal_u64 {
            Ok(Expr::Literal(ScalarValue::UInt64(Some(
                self.literal_uint as u64,
            ))))
        } else if let Some(aggregate_expr) = &self.aggregate_expr {
            let fun = match aggregate_expr.aggr_function {
                f if f == protobuf::AggregateFunction::Min as i32 => {
                    Ok(aggregates::AggregateFunction::Min)
                }
                f if f == protobuf::AggregateFunction::Max as i32 => {
                    Ok(aggregates::AggregateFunction::Max)
                }
                f if f == protobuf::AggregateFunction::Sum as i32 => {
                    Ok(aggregates::AggregateFunction::Sum)
                }
                f if f == protobuf::AggregateFunction::Avg as i32 => {
                    Ok(aggregates::AggregateFunction::Avg)
                }
                f if f == protobuf::AggregateFunction::Count as i32 => {
                    Ok(aggregates::AggregateFunction::Count)
                }
                other => Err(internal_err!(
                    "Unsupported aggregate function '{:?}'",
                    other
                )),
            }?;
            // TODO what about distinct ???
            Ok(Expr::AggregateFunction {
                fun,
                args: vec![parse_required_expr(&aggregate_expr.expr)?],
                distinct: false,
            })
        } else if let Some(alias) = &self.alias {
            Ok(Expr::Alias(
                Box::new(parse_required_expr(&alias.expr)?),
                alias.alias.clone(),
            ))
        } else {
            Err(internal_err!("Unsupported logical expression '{:?}'", self))
        }
    }
}

fn from_proto_binary_op(op: &str) -> Result<Operator, BuzzError> {
    match op {
        "Eq" => Ok(Operator::Eq),
        "NotEq" => Ok(Operator::NotEq),
        "LtEq" => Ok(Operator::LtEq),
        "Lt" => Ok(Operator::Lt),
        "Gt" => Ok(Operator::Gt),
        "GtEq" => Ok(Operator::GtEq),
        "Plus" => Ok(Operator::Plus),
        "Minus" => Ok(Operator::Minus),
        "Multiply" => Ok(Operator::Multiply),
        "Divide" => Ok(Operator::Divide),
        other => Err(internal_err!("Unsupported binary operator '{:?}'", other)),
    }
}

fn parse_required_expr(
    p: &Option<Box<protobuf::LogicalExprNode>>,
) -> Result<Expr, BuzzError> {
    match p {
        Some(expr) => expr.as_ref().try_into(),
        None => Err(internal_err!("Missing required expression")),
    }
}
