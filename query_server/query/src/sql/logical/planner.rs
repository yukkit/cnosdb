use crate::sql::planner::SqlPlaner;
use std::sync::Arc;

use datafusion::{
    common::DFField,
    error::DataFusionError,
    logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, Projection, TableSource},
    prelude::{cast, lit, Expr},
    scalar::ScalarValue,
};
use spi::query::logical_planner::{
    affected_row_expr, merge_affected_row_expr, MISMATCHED_COLUMNS, MISSING_COLUMN,
};

use datafusion::common::Result as DFResult;
use trace::debug;

use crate::extension::logical::plan_node::table_writer::TableWriterPlanNode;

pub type DefaultLogicalPlanner<S> = SqlPlaner<S>;

pub trait TableWriteExt {
    fn write(
        self,
        target_table: Arc<dyn TableSource>,
        table_name: &str,
        insert_columns: Vec<String>,
    ) -> DFResult<Self>
    where
        Self: Sized;
}

impl TableWriteExt for LogicalPlanBuilder {
    fn write(
        self,
        target_table: Arc<dyn TableSource>,
        table_name: &str,
        insert_columns: Vec<String>,
    ) -> DFResult<Self> {
        let source_plan = self.build()?;
        // Check if the plan is legal
        semantic_check(insert_columns.as_ref(), &source_plan, target_table.clone())?;

        let final_source_logical_plan = add_projection_between_source_and_insert_node_if_necessary(
            target_table.clone(),
            source_plan,
            insert_columns,
        )?;

        let plan = table_write_plan_node(table_name, target_table, final_source_logical_plan)?;

        Ok(Self::from(plan))
    }
}

fn semantic_check(
    insert_columns: &[String],
    source_plan: &LogicalPlan,
    target_table: Arc<dyn TableSource>,
) -> DFResult<()> {
    let target_table_schema = target_table.schema();
    let target_table_fields = target_table_schema.fields();

    let source_field_num = source_plan.schema().fields().len();
    let insert_field_num = insert_columns.len();
    let target_table_field_num = target_table_fields.len();

    if insert_field_num > source_field_num {
        return Err(DataFusionError::Plan(MISMATCHED_COLUMNS.to_string()));
    }

    if insert_field_num == 0 && source_field_num != target_table_field_num {
        return Err(DataFusionError::Plan(MISMATCHED_COLUMNS.to_string()));
    }
    // The target table must contain all insert fields
    for insert_col in insert_columns {
        target_table_fields
            .iter()
            .find(|e| e.name() == insert_col)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "{} {}, expected: {}",
                    MISSING_COLUMN,
                    insert_col,
                    target_table_fields
                        .iter()
                        .map(|e| e.name().as_str())
                        .collect::<Vec<&str>>()
                        .join(",")
                ))
            })?;
    }

    Ok(())
}

/// Add a projection operation (if necessary)
/// 1. Iterate over all fields of the table
///   1.1. Construct the col expression
///   1.2. Check if the current field exists in columns
///     1.2.1. does not exist: add cast(null as target_type) expression to save
///     1.2.1. Exist: save if the type matches, add cast(expr as target_type) to save if it does not exist
fn add_projection_between_source_and_insert_node_if_necessary(
    target_table: Arc<dyn TableSource>,
    source_plan: LogicalPlan,
    insert_columns: Vec<String>,
) -> DFResult<LogicalPlan> {
    let insert_col_name_with_source_field_tuples: Vec<(&String, &DFField)> = insert_columns
        .iter()
        .zip(source_plan.schema().fields())
        .collect();

    debug!(
        "Insert col name with source field tuples: {:?}",
        insert_col_name_with_source_field_tuples
    );
    debug!("Target table: {:?}", target_table.schema());

    let assignments: Vec<Expr> = target_table
        .schema()
        .fields()
        .iter()
        .map(|column| {
            let target_column_name = column.name();
            let target_column_data_type = column.data_type();

            let expr = if let Some((_, source_field)) = insert_col_name_with_source_field_tuples
                .iter()
                .find(|(insert_col_name, _)| *insert_col_name == target_column_name)
            {
                // insert column exists in the target table
                if source_field.data_type() == target_column_data_type {
                    // save if type matches col(source_field_name)
                    Expr::Column(source_field.qualified_column())
                } else {
                    // Add cast(source_col as target_type) if it doesn't exist
                    cast(
                        Expr::Column(source_field.qualified_column()),
                        target_column_data_type.clone(),
                    )
                }
            } else {
                // The specified column in the target table is missing from the insert
                // then add cast(null as target_type)
                cast(lit(ScalarValue::Null), target_column_data_type.clone())
            };

            expr.alias(target_column_name)
        })
        .collect();

    debug!("assignments: {:?}", &assignments);

    Ok(LogicalPlan::Projection(Projection::try_new(
        assignments,
        Arc::new(source_plan),
        None,
    )?))
}

fn table_write_plan_node(
    table_name: impl Into<String>,
    target_table: Arc<dyn TableSource>,
    input: LogicalPlan,
) -> DFResult<LogicalPlan> {
    // output variable for insert operation
    let expr = input
        .schema()
        .fields()
        .iter()
        .last()
        .map(|e| Expr::Column(e.qualified_column()));

    debug_assert!(
        expr.is_some(),
        "invalid table write node's input logical plan"
    );

    let expr = unsafe { expr.unwrap_unchecked() };

    let affected_row_expr = affected_row_expr(expr);

    // construct table writer logical node
    let node = Arc::new(TableWriterPlanNode::try_new(
        table_name.into(),
        target_table,
        Arc::new(input),
        vec![affected_row_expr],
    )?);

    let df_plan = LogicalPlan::Extension(Extension { node });

    let group_expr: Vec<Expr> = vec![];

    LogicalPlanBuilder::from(df_plan)
        .aggregate(group_expr, vec![merge_affected_row_expr()])?
        .build()
}
