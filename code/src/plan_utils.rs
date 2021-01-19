use datafusion::logical_plan::{self, Expr, Operator};

/// converts "A AND (B AND (C OR D))" => [A, B, C OR D]
/// Copied from DataFusion filter pushdown
pub fn split_expr<'a>(predicate: &'a Expr, predicates: &mut Vec<&'a Expr>) {
    match predicate {
        Expr::BinaryExpr {
            right,
            op: Operator::And,
            left,
        } => {
            split_expr(&left, predicates);
            split_expr(&right, predicates);
        }
        other => predicates.push(other),
    }
}

/// converts [A, B, C] => "(A AND B) AND C"
pub fn merge_expr<'a>(predicates: &[Expr]) -> Expr {
    let mut predicates_iter = predicates.iter();
    let mut merged_pred = predicates_iter
        .next()
        .expect("Merging requires at least one expr")
        .clone();
    while let Some(expr) = predicates_iter.next() {
        merged_pred = logical_plan::and(merged_pred, expr.clone());
    }
    merged_pred
}
