use std::convert::TryInto;
use std::sync::Arc;

use crate::catalog::SizedFile;
use crate::datasource::ParquetTable;
use crate::error::BuzzError;
use crate::internal_err;
use crate::protobuf;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::logical_plan::{
    Expr, LogicalPlan, LogicalPlanBuilder, Operator, TableSource,
};
use datafusion::physical_plan::aggregates;
use datafusion::scalar::ScalarValue;

macro_rules! convert_required {
    ($PB:expr) => {{
        if let Some(field) = $PB.as_ref() {
            field.try_into()
        } else {
            Err(internal_err!("Missing required field in protobuf"))
        }
    }};
}

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
            let schema: Schema = convert_required!(scan.schema)?;
            let schema_ref = Arc::new(schema);
            let provider = ParquetTable::new(
                scan.region.to_owned(),
                scan.bucket.to_owned(),
                scan.files
                    .iter()
                    .map(|sized_file| SizedFile {
                        key: sized_file.key.to_owned(),
                        length: sized_file.length,
                    })
                    .collect(),
                schema_ref.clone(),
            );
            Ok(LogicalPlan::TableScan {
                schema_name: "".to_string(),
                source: TableSource::FromProvider(Arc::new(provider)),
                table_schema: schema_ref.clone(),
                projected_schema: schema_ref,
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

fn from_proto_arrow_type(dt: i32) -> Result<DataType, BuzzError> {
    match dt {
        dt if dt == protobuf::ArrowType::Uint8 as i32 => Ok(DataType::UInt8),
        dt if dt == protobuf::ArrowType::Int8 as i32 => Ok(DataType::Int8),
        dt if dt == protobuf::ArrowType::Uint16 as i32 => Ok(DataType::UInt16),
        dt if dt == protobuf::ArrowType::Int16 as i32 => Ok(DataType::Int16),
        dt if dt == protobuf::ArrowType::Uint32 as i32 => Ok(DataType::UInt32),
        dt if dt == protobuf::ArrowType::Int32 as i32 => Ok(DataType::Int32),
        dt if dt == protobuf::ArrowType::Uint64 as i32 => Ok(DataType::UInt64),
        dt if dt == protobuf::ArrowType::Int64 as i32 => Ok(DataType::Int64),
        dt if dt == protobuf::ArrowType::Float as i32 => Ok(DataType::Float32),
        dt if dt == protobuf::ArrowType::Double as i32 => Ok(DataType::Float64),
        dt if dt == protobuf::ArrowType::Utf8 as i32 => Ok(DataType::Utf8),
        other => Err(internal_err!("Unsupported data type {:?}", other)),
    }
}

impl TryInto<Schema> for &protobuf::Schema {
    type Error = BuzzError;

    fn try_into(self) -> Result<Schema, Self::Error> {
        let fields = self
            .columns
            .iter()
            .map(|c| {
                let dt: Result<DataType, _> = from_proto_arrow_type(c.arrow_type);
                dt.and_then(|dt| Ok(Field::new(&c.name, dt, c.nullable)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Schema::new(fields))
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
