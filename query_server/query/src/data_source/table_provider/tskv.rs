use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use coordinator::service::CoordinatorRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DFSchemaRef;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::logical_plan::AggWithGrouping;
use datafusion::logical_expr::{
    aggregate_function, Expr, TableProviderAggregationPushDown, TableProviderFilterPushDown,
};
use datafusion::optimizer::utils::split_conjunction;
use datafusion::physical_plan::{project_schema, ExecutionPlan};
use meta::error::MetaError;
use models::predicate::domain::{Predicate, PredicateRef, PushedAggregateFunction};
use models::schema::{TskvTableSchema, TskvTableSchemaRef};
use trace::debug;

use crate::data_source::sink::tskv::TskvRecordBatchSinkProvider;
use crate::data_source::WriteExecExt;
use crate::extension::expr::expr_utils;
use crate::extension::physical::plan_node::aggregate_filter_scan::AggregateFilterTskvExec;
use crate::extension::physical::plan_node::table_writer::TableWriterExec;
use crate::extension::physical::plan_node::tag_scan::TagScanExec;
use crate::extension::physical::plan_node::tskv_exec::TskvExec;

#[derive(Clone)]
pub struct ClusterTable {
    coord: CoordinatorRef,
    schema: TskvTableSchemaRef,
}

impl ClusterTable {
    async fn create_physical_plan(
        &self,
        projection: Option<&Vec<usize>>,
        predicate: PredicateRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let proj_schema = self.project_schema(projection)?;

        Ok(Arc::new(TskvExec::new(
            self.schema.clone(),
            proj_schema,
            predicate,
            self.coord.clone(),
        )))
    }

    pub fn new(coord: CoordinatorRef, schema: TskvTableSchemaRef) -> Self {
        ClusterTable { coord, schema }
    }

    pub async fn tag_scan(
        &self,
        _ctx: &SessionState,
        projected_schema: &DFSchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filter = Arc::new(
            Predicate::default()
                .set_limit(limit)
                .push_down_filter(filters, &self.schema),
        );

        Ok(Arc::new(TagScanExec::new(
            self.schema.clone(),
            Arc::new(projected_schema.as_ref().into()),
            filter,
            self.coord.clone(),
        )))
    }

    pub fn table_schema(&self) -> TskvTableSchemaRef {
        self.schema.clone()
    }

    // Check and return the projected schema
    fn project_schema(&self, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
        valid_project(&self.schema, projection)
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        project_schema(&self.schema.to_arrow_schema(), projection)
    }
}

#[async_trait]
impl TableProvider for ClusterTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.to_arrow_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        agg_with_grouping: Option<&AggWithGrouping>,
        // limit can be used to reduce the amount scanned
        // from the datasource as a performance optimization.
        // If set, it contains the amount of rows needed by the `LogicalPlan`,
        // The datasource should return *at least* this number of rows if available.
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filter = Arc::new(
            Predicate::default()
                .set_limit(limit)
                .push_down_filter(filters, &self.schema),
        );

        if let Some(agg_with_grouping) = agg_with_grouping {
            debug!("Create aggregate filter tskv scan.");
            return create_agg_filter_scan(
                self.coord.clone(),
                self.schema.clone(),
                filter,
                agg_with_grouping,
            );
        }

        return self.create_physical_plan(projection, filter).await;
    }

    fn supports_filter_pushdown(&self, expr: &Expr) -> Result<TableProviderFilterPushDown> {
        let exprs = split_conjunction(expr);
        let exprs = exprs.into_iter().cloned().collect::<Vec<_>>();
        if expr_utils::find_exprs_in_exprs(&exprs, &|nested_expr| {
            !expr_utils::is_time_filter(nested_expr)
        })
        .is_empty()
        {
            // all exprs are time range filter
            return Ok(TableProviderFilterPushDown::Exact);
        }

        Ok(TableProviderFilterPushDown::Inexact)
    }

    fn supports_aggregate_pushdown(
        &self,
        group_expr: &[Expr],
        aggr_expr: &[Expr],
    ) -> Result<TableProviderAggregationPushDown> {
        if !group_expr.is_empty() {
            return Ok(TableProviderAggregationPushDown::Unsupported);
        }

        let result = if aggr_expr.iter().all(|e| {
            match e {
                Expr::AggregateFunction(AggregateFunction {
                    fun,
                    args,
                    distinct,
                    filter,
                }) => {
                    let support_agg_func = matches!(
                        fun,
                        aggregate_function::AggregateFunction::Count // TODO
                                                                     // | aggregate_function::AggregateFunction::Max
                                                                     // | aggregate_function::AggregateFunction::Min
                                                                     // | aggregate_function::AggregateFunction::Sum
                    );

                    support_agg_func
                        && args.len() == 1
                        && matches!(args[0], Expr::Column(_))
                        && !distinct
                        && filter.is_none()
                }
                _ => false,
            }
        }) {
            TableProviderAggregationPushDown::Ungrouped
        } else {
            TableProviderAggregationPushDown::Unsupported
        };

        Ok(result)
    }
}

fn create_agg_filter_scan(
    coord: CoordinatorRef,
    table_schema: TskvTableSchemaRef,
    filter: Arc<Predicate>,
    agg_with_grouping: &AggWithGrouping,
) -> Result<Arc<dyn ExecutionPlan>> {
    // TODO only extract time range from filter

    let AggWithGrouping {
        group_expr: _,
        agg_expr,
        schema,
    } = agg_with_grouping;

    let aggs = agg_expr
        .iter()
        .map(|e| match e {
            Expr::AggregateFunction(agg) => Ok(agg),
            _ => Err(DataFusionError::Plan(
                "Invalid plan, pushed aggregate functions contains unsupported".to_string(),
            )),
        })
        .collect::<Result<Vec<_>>>()?;

    let pushed_aggs = aggs
        .into_iter()
        .map(|agg| {
            let AggregateFunction { fun, args, .. } = agg;

            args.iter()
                .map(|expr| {
                    // The parameter of the aggregate function pushed down must be a column column
                    match expr {
                        Expr::Column(c) => Ok(c),
                        _ => Err(DataFusionError::Internal(format!(
                            "Pushed aggregate functions's args contains non-column: {expr:?}."
                        ))),
                    }
                })
                .collect::<Result<Vec<_>>>()
                .and_then(|columns| {
                    // Convert pushdown aggregate functions to intermediate structures
                    match fun {
                        aggregate_function::AggregateFunction::Count => {
                            let column = columns
                                .first()
                                .ok_or_else(|| {
                                    DataFusionError::Internal(
                                        "Pushed aggregate functions's args is none.".to_string(),
                                    )
                                })?
                                .deref()
                                .clone();
                            Ok(PushedAggregateFunction::Count(column.name))
                        }
                        // aggregate_function::AggregateFunction::Max => {},
                        // aggregate_function::AggregateFunction::Min => {},
                        _ => Err(DataFusionError::Internal(
                            "Pushed aggregate functions's args is none.".to_string(),
                        )),
                    }
                })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(AggregateFilterTskvExec::new(
        coord,
        SchemaRef::from(schema.deref()),
        table_schema,
        pushed_aggs,
        filter,
    )))
}

#[async_trait]
impl WriteExecExt for ClusterTable {
    async fn write(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<TableWriterExec>> {
        let record_batch_sink_privider = Arc::new(TskvRecordBatchSinkProvider::new(
            self.coord.clone(),
            self.schema.clone(),
        ));

        Ok(Arc::new(TableWriterExec::new(
            input,
            self.schema.name.clone(),
            record_batch_sink_privider,
        )))
    }
}

/// Check the validity of the projection
///
/// 1. If the projection contains the time column, it must contain the field column, otherwise an error will be reported
pub fn valid_project(
    schema: &TskvTableSchema,
    projection: Option<&Vec<usize>>,
) -> std::result::Result<(), MetaError> {
    let mut field_count = 0;
    let mut contains_time_column = false;

    if let Some(e) = projection {
        e.iter().cloned().for_each(|idx| {
            if let Some(c) = schema.column_by_index(idx) {
                if c.column_type.is_time() {
                    contains_time_column = true;
                }
                if c.column_type.is_field() {
                    field_count += 1;
                }
            };
        });
    }

    if contains_time_column && field_count == 0 {
        return Err(MetaError::CommonError {
            msg: "If the projection contains the time column, it must contain the field column"
                .to_string(),
        });
    }

    Ok(())
}
