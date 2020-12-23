use std::time::Instant;

use super::hbee_scheduler::HBeeScheduler;
use super::hcomb_manager::HCombManager;
use super::hcomb_scheduler::HCombScheduler;
use super::query_planner::QueryPlanner;
use crate::datasource::CatalogTable;
use crate::error::Result;
use crate::models::query::BuzzQuery;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use chrono::Utc;
use futures::{StreamExt, TryStreamExt};
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
        println!("[fuse] schedule hcombs");
        let future_hcombs = (0..plan.zones.len()).map(|i| {
            self.hcomb_scheduler
                .schedule(&addresses[i], plan.zones[i].hcomb.clone())
        });
        let hcomb_streams = futures::stream::iter(future_hcombs)
            .buffer_unordered(10)
            .try_collect::<Vec<_>>()
            .await?;

        // when hcombs are ready, start hbees!
        // TODO start sending to combs as soon as they are ready
        // TODO alternate between combs?
        println!("[fuse] schedule hbees");
        let future_hbees = (0..plan.zones.len())
            .flat_map(|i| (0..plan.zones[i].hbee.len()).map(move |j| (i, j)))
            .map(|(i, j)| {
                self.hbee_scheduler.schedule(
                    query_id.clone(),
                    &addresses[i],
                    plan.zones[i].hbee[j].clone(),
                )
            });
        futures::stream::iter(future_hbees)
            .buffer_unordered(10)
            .try_collect::<Vec<_>>()
            .await?;

        // wait for hcombs to collect all the results and desplay them comb by comb
        println!("[fuse] collect hcombs");
        for hcomb_stream in hcomb_streams {
            let result: Vec<RecordBatch> = hcomb_stream.collect::<Vec<_>>().await;
            pretty::print_batches(&result).unwrap();
        }

        println!("[fuse] run duration: {}", start.elapsed().as_millis());
        Ok(())
    }
}
