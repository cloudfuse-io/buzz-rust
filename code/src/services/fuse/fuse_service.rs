use std::time::Instant;

use super::hbee_scheduler::HBeeScheduler;
use super::hcomb_manager::HCombManager;
use super::hcomb_scheduler::HCombScheduler;
use super::query_planner::QueryPlanner;
use crate::bad_req_err;
use crate::datasource::{CatalogTable, DeltaCatalogTable};
use crate::error::Result;
use crate::example_catalog;
use crate::models::query::{BuzzCatalog, BuzzCatalogType, BuzzQuery};
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

    pub async fn configure_catalog(&mut self, configs: &Vec<BuzzCatalog>) -> Result<()> {
        // TODO check definition validity
        for conf in configs {
            let catalog = match &conf.r#type {
                BuzzCatalogType::Static => match &conf.uri[..] {
                    "nyc_taxi_ursa" => example_catalog::nyc_taxi_ursa(),
                    "nyc_taxi_cloudfuse" => example_catalog::nyc_taxi_cloudfuse(),
                    "nyc_taxi_cloudfuse_sample" => {
                        example_catalog::nyc_taxi_cloudfuse_sample()
                    }
                    _ => return Err(bad_req_err!("Static catalog not found")),
                },
                BuzzCatalogType::DeltaLake => CatalogTable::new(Box::new(
                    DeltaCatalogTable::try_new(&conf.uri, "us-east-2".to_owned()).await?,
                )),
            };
            self.query_planner.add_catalog(&conf.name, catalog)?
        }
        Ok(())
    }

    pub async fn run(&mut self, query: BuzzQuery) -> Result<()> {
        self.configure_catalog(&query.catalogs).await?;
        let start_run = Instant::now();
        let addresses_future = self.hcomb_manager.find_or_start(&query.capacity);
        let query_id = format!("query-{}", Utc::now().to_rfc3339());
        let plan_future =
            self.query_planner
                .plan(query_id.clone(), query.steps, query.capacity.zones);
        let (addresses, plan) = join!(addresses_future, plan_future);
        let addresses = addresses?;
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
            self.hcomb_scheduler.schedule(
                &addresses[i],
                &plan.zones[i].hcomb.table,
                plan.zones[i].hcomb.sql.clone(),
                plan.zones[i].hcomb.source.clone(),
            )
        });
        let hcomb_streams = futures::stream::iter(future_hcombs)
            .buffer_unordered(10)
            .try_collect::<Vec<_>>()
            .await?;

        // when hcombs are ready, start hbees!
        // TODO start hbees for hcombs that are ready before the others?
        println!("[fuse] schedule {} hbees", plan.nb_hbee);
        let start_schedule = Instant::now();
        let mut hcomb_hbee_idx_tuple = (0..plan.zones.len())
            .flat_map(|i| (0..plan.zones[i].hbee.len()).map(move |j| (i, j)))
            .collect::<Vec<_>>();

        // sort by hbee index in order to alternate between hcombs
        hcomb_hbee_idx_tuple.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        let future_hbees = hcomb_hbee_idx_tuple.into_iter().map(|(i, j)| {
            self.hbee_scheduler.schedule(
                query_id.clone(),
                &addresses[i],
                &plan.zones[i].hbee[j].table,
                plan.zones[i].hbee[j].sql.clone(),
                plan.zones[i].hbee[j].source.clone(),
            )
        });
        futures::stream::iter(future_hbees)
            .buffer_unordered(10)
            .try_collect::<Vec<_>>()
            .await?;

        println!(
            "[fuse] hbee scheduling duration: {}",
            start_schedule.elapsed().as_millis()
        );

        // wait for hcombs to collect all the results and desplay them comb by comb
        println!("[fuse] collect hcombs");
        for hcomb_stream in hcomb_streams {
            let result: Vec<RecordBatch> = hcomb_stream.try_collect::<Vec<_>>().await?;
            pretty::print_batches(&result).unwrap();
        }

        println!(
            "[fuse] hbee total duration: {}",
            start_schedule.elapsed().as_millis()
        );
        println!(
            "[fuse] total run duration: {}",
            start_run.elapsed().as_millis()
        );
        Ok(())
    }
}
