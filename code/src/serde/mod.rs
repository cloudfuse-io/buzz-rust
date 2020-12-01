pub mod from_proto;
pub mod to_proto;

#[cfg(test)]
mod tests {
    use crate::protobuf;
    use datafusion::logical_plan::{col, lit, LogicalPlan, LogicalPlanBuilder};
    use std::convert::TryInto;

    #[test]
    fn roundtrip() -> std::result::Result<(), Box<dyn std::error::Error>> {
        // let schema = Schema::new(vec![
        //     Field::new("id", DataType::Int32, false),
        //     Field::new("first_name", DataType::Utf8, false),
        //     Field::new("last_name", DataType::Utf8, false),
        //     Field::new("state", DataType::Utf8, false),
        //     Field::new("salary", DataType::Int32, false),
        // ]);

        let source_plan = LogicalPlanBuilder::scan_parquet("employee.csv", None)
            .and_then(|plan| plan.filter(col("state").eq(lit("CO"))))
            .and_then(|plan| plan.project(vec![col("id")]))
            .and_then(|plan| plan.build())
            .unwrap();

        let proto: protobuf::LogicalPlanNode = (&source_plan).try_into()?;

        let transfered_plan: LogicalPlan = (&proto).try_into()?;

        assert_eq!(
            format!("{:?}", source_plan),
            format!("{:?}", transfered_plan)
        );

        Ok(())
    }

    // #[test]
    // fn roundtrip_aggregate() -> Result<()> {
    //     let schema = Schema::new(vec![
    //         Field::new("id", DataType::Int32, false),
    //         Field::new("first_name", DataType::Utf8, false),
    //         Field::new("last_name", DataType::Utf8, false),
    //         Field::new("state", DataType::Utf8, false),
    //         Field::new("salary", DataType::Int32, false),
    //     ]);

    //     let plan = LogicalPlanBuilder::scan_csv(
    //         "employee.csv",
    //         CsvReadOptions::new().schema(&schema).has_header(true),
    //         None,
    //     )
    //     .and_then(|plan| plan.aggregate(vec![col("state")], vec![max(col("salary"))]))
    //     .and_then(|plan| plan.build())
    //     .unwrap();

    //     let action = &Action::InteractiveQuery {
    //         plan: plan.clone(),
    //         settings: HashMap::new(),
    //         // tables: vec![TableMeta::Csv {
    //         //     table_name: "employee".to_owned(),
    //         //     has_header: true,
    //         //     path: "/foo/bar.csv".to_owned(),
    //         //     schema: schema.clone(),
    //         // }],
    //     };

    //     let proto: protobuf::Action = action.try_into()?;

    //     let action2: Action = (&proto).try_into()?;

    //     assert_eq!(format!("{:?}", action), format!("{:?}", action2));

    //     Ok(())
    // }

    // fn max(expr: Expr) -> Expr {
    //     Expr::AggregateFunction {
    //         name: "MAX".to_owned(),
    //         args: vec![expr],
    //     }
    // }
}
