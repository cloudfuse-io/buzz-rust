use std::time::Instant;

use crate::datasource::CatalogTable;
use crate::error::Result;
use crate::hbee_scheduler::HBeeScheduler;
use crate::hcomb_manager::HCombManager;
use crate::hcomb_scheduler::HCombScheduler;
use crate::query::BuzzQuery;
use crate::query_planner::QueryPlanner;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use chrono::Utc;
use futures::StreamExt;
use tokio::join;

pub struct FuseService {
    hbee_scheduler: Box<dyn HBeeScheduler>,
    hcomb_manager: Box<dyn HCombManager>,
    hcomb_scheduler: Box<dyn HCombScheduler>,
    query_planner: QueryPlanner,
}

impl FuseService {
    pub fn new(
        hbee_scheduler: Box<dyn HBeeScheduler>,
        hcomb_manager: Box<dyn HCombManager>,
        hcomb_scheduler: Box<dyn HCombScheduler>,
        query_planner: QueryPlanner,
    ) -> Self {
        FuseService {
            hbee_scheduler,
            hcomb_manager,
            hcomb_scheduler,
            query_planner,
        }
    }

    pub fn add_catalog(&mut self, name: &str, table: CatalogTable) {
        self.query_planner.add_catalog(name, table);
    }

    pub async fn run(&mut self, query: BuzzQuery) -> Result<()> {
        let start = Instant::now();
        let addresses_future = self.hcomb_manager.find_or_start(&query.capacity);
        let query_id = format!("query-{}", Utc::now().to_rfc3339());
        let plan_future =
            self.query_planner
                .plan(query_id.clone(), query.steps, query.capacity.zones);
        let (addresses, plan) = join!(addresses_future, plan_future);
        let plan = plan?;

        assert!(
            addresses.len() >= plan.zones.len(),
            "Not enough hcombs (found {}) were started for plan (expected {})",
            addresses.len(),
            plan.zones.len()
        );

        if plan.zones.len() == 0 {
            println!("no work scheduled, empty result");
            return Ok(());
        }

        // connect to the hcombs to init the query and get result handle
        // TODO connect in //
        let mut result_streams = vec![];
        println!("[fuse] schedule hcombs");
        for i in 0..plan.zones.len() {
            let batch_stream = self
                .hcomb_scheduler
                .schedule(&addresses[i], plan.zones[i].hcomb.clone())
                .await?;
            result_streams.push(batch_stream);
        }
        // when hcombs are ready, start hbees!
        // TODO start sending to combs as soon as they are ready
        // TODO alternate between combs?
        // TODO schedule multiple in //
        println!("[fuse] schedule hbees");
        for i in 0..plan.zones.len() {
            for j in 0..plan.zones[i].hbee.len() {
                self.hbee_scheduler
                    .schedule(
                        query_id.clone(),
                        &addresses[i],
                        plan.zones[i].hbee[j].clone(),
                    )
                    .await?;
            }
        }

        // wait for hcombs to collect all the results and desplay them comb by comb
        println!("[fuse] collect hcombs");
        for result_stream in result_streams {
            let result: Vec<RecordBatch> = result_stream.collect::<Vec<_>>().await;
            pretty::print_batches(&result).unwrap();
        }

        println!("[fuse] run duration: {}", start.elapsed().as_millis());
        Ok(())
    }
}
