use std::time::Duration;

use prometheus;
use tokio::prelude::*;
use tokio::timer::Interval;

pub fn metrics_collector(address: String, push_interval: u64) -> impl Future<Item = (), Error = ()> {
    Interval::new_interval(Duration::from_millis(push_interval))
        .for_each(move |_| {
            let metric_families = prometheus::gather();
            let push_result = prometheus::push_metrics(
                "cortex-dispatcher",
                labels! {},
                &address,
                metric_families,
                Some(prometheus::BasicAuthentication {
                    username: "user".to_owned(),
                    password: "pass".to_owned(),
                }),
            );

            match push_result {
                Ok(_) => {
                    debug!("Pushed metrics to Prometheus Gateway");
                }
                Err(e) => {
                    error!("Error pushing metrics to Prometheus Gateway: {}", e);
                }
            };

            future::ok(())
        })
        .map_err(|e| {
            error!("{}", e);
        })
}
