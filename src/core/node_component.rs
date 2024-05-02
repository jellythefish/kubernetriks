//! Node component simulates a real node running pods.

use std::cell::RefCell;
use std::rc::Rc;

use dslab_core::{cast, Event, EventHandler, SimulationContext};

use crate::core::common::SimComponentId;
use crate::core::events::{
    BindPodToNodeRequest, NodeRemovedFromCluster, PodFinishedRunning, PodStartedRunning,
    RemoveNodeRequest,
};
use crate::core::node::Node;
use crate::core::pod::PodConditionType;

use crate::config::SimulationConfig;

use crate::metrics::collector::MetricsCollector;

pub struct NodeComponent {
    ctx: SimulationContext,
    // Initialized later when the node component is actually allocated from node pool.
    // Sets to None when the node gets back to node pool.
    pub runtime: Option<NodeRuntime>,

    /// Flag to check that node is being removed so cannot accept any pod occasionally.
    pub removed: bool,

    metrics_collector: Rc<RefCell<MetricsCollector>>,
}

pub struct NodeRuntime {
    pub api_server: SimComponentId,
    pub node: Node,
    pub config: Rc<SimulationConfig>,
}

impl NodeComponent {
    pub fn new(ctx: SimulationContext, metrics_collector: Rc<RefCell<MetricsCollector>>) -> Self {
        Self {
            ctx,
            runtime: None,
            removed: false,
            metrics_collector,
        }
    }

    pub fn id(&self) -> SimComponentId {
        self.ctx.id()
    }

    pub fn node_name(&self) -> &str {
        &self.runtime.as_ref().unwrap().node.metadata.name
    }

    pub fn get_node(&self) -> &Node {
        &self.runtime.as_ref().unwrap().node
    }

    pub fn context_name(&self) -> &str {
        &self.ctx.name()
    }

    /// This method cancels events `PodFinishedRunning` of a current node which were submitted to
    /// the simulation queue and which delay is >= cancellation_time.
    /// This method is not cheap and we assume that node removals and pod cancellations happen not
    /// too often as pod creations and running. Thus, we iterate through the whole queue of
    /// simulation events with callback returning `true` if should cancel event.
    fn cancel_running_pods(&mut self, cancellation_time: f64) {
        let current_node_name = &self.runtime.as_ref().unwrap().node.metadata.name;
        self.ctx.cancel_events(|event| {
            if let Some(running_pod) = event.data.downcast_ref::<PodFinishedRunning>() {
                if running_pod.finish_time >= cancellation_time
                    && running_pod.node_name == *current_node_name
                {
                    return true;
                }
            }
            return false;
        });
    }

    pub fn simulate_pod_runtime(&mut self, event_time: f64, pod_name: String, pod_duration: f64) {
        let delay = pod_duration
            + self
                .runtime
                .as_ref()
                .unwrap()
                .config
                .as_to_node_network_delay;
        self.ctx.emit(
            PodFinishedRunning {
                pod_name,
                node_name: self.runtime.as_ref().unwrap().node.metadata.name.clone(),
                finish_time: event_time + pod_duration,
                finish_result: PodConditionType::PodSucceeded,
            },
            self.runtime.as_ref().unwrap().api_server,
            delay,
        );
    }
}

impl EventHandler for NodeComponent {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            BindPodToNodeRequest {
                pod_name,
                pod_duration,
                node_name,
            } => {
                assert!(
                    !self.removed,
                    "Pod is assigned on node which is being removed, looks like a bug."
                );
                assert_eq!(
                    node_name,
                    self.node_name(),
                    "Pod is assigned to node with different node 
                name: pod - {:?}, current node - {:?}, assigned node - {:?}",
                    pod_name,
                    self.node_name(),
                    node_name
                );
                self.simulate_pod_runtime(event.time, pod_name.clone(), pod_duration);

                self.ctx.emit(
                    PodStartedRunning {
                        pod_name,
                        start_time: event.time,
                    },
                    self.runtime.as_ref().unwrap().api_server,
                    self.runtime
                        .as_ref()
                        .unwrap()
                        .config
                        .as_to_node_network_delay,
                );
            }
            RemoveNodeRequest { node_name } => {
                self.removed = true;
                assert_eq!(
                    node_name,
                    self.node_name(),
                    "Trying to remove other node than self: {:?} vs {:?}",
                    node_name,
                    self.node_name()
                );
                // Here we should cancel all events which have been already submitted to simulation
                // queue as running events as we terminate.
                self.cancel_running_pods(event.time);

                self.ctx.emit(
                    NodeRemovedFromCluster {
                        removal_time: event.time,
                        node_name,
                    },
                    self.runtime.as_ref().unwrap().api_server,
                    self.runtime
                        .as_ref()
                        .unwrap()
                        .config
                        .as_to_node_network_delay,
                );
            }
        });
    }
}
