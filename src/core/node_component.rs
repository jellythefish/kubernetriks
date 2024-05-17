//! Node component simulates a real node running pods.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use dslab_core::event::EventId;
use dslab_core::{cast, Event, EventHandler, SimulationContext};

use crate::core::common::{RuntimeResourcesUsageModelConfig, SimComponentId};
use crate::core::events::{
    BindPodToNodeRequest, NodeRemovedFromCluster, PodFinishedRunning, PodRemovedFromNode,
    PodStartedRunning, RemoveNodeRequest, RemovePodRequest,
};
use crate::core::node::Node;
use crate::core::pod::PodConditionType;

use crate::core::resource_usage::helpers::resource_usage_model_from_config;
use crate::core::resource_usage::interface::ResourceUsageModel;

use crate::config::SimulationConfig;
use crate::metrics::collector::MetricsCollector;

pub struct RunningPodInfo {
    pub event_id: Option<EventId>,
    /// Name of pod group a pod belongs to. None if a pod is not in group.
    pub pod_group: Option<String>,
    pub cpu_usage_model: Option<Box<dyn ResourceUsageModel>>,
    pub ram_usage_model: Option<Box<dyn ResourceUsageModel>>,
}

pub struct NodeComponent {
    ctx: SimulationContext,
    // Initialized later when the node component is actually allocated from node pool.
    // Sets to None when the node gets back to node pool.
    pub runtime: Option<NodeRuntime>,

    /// Map from pod name to its finishing event `PodFinishedRunning` id which is sent to self.
    pub running_pods: HashMap<String, RunningPodInfo>,
    /// Set of canceled running nodes which did not finish due to node removal.
    pub canceled_pods: HashSet<String>,

    /// Flag to check that node is being removed so cannot accept any pod occasionally.
    pub removed: bool,
    pub removal_time: f64,

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
            running_pods: Default::default(),
            canceled_pods: Default::default(),
            removed: false,
            removal_time: 0.0,
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
    /// the simulation queue and which delay is >= current cancellation time.
    fn cancel_all_running_pods(&mut self) {
        for (pod_name, info) in self.running_pods.iter() {
            self.canceled_pods.insert(pod_name.to_string());
            if let Some(event_id) = info.event_id {
                self.ctx.cancel_event(event_id);
            }
        }

        self.running_pods.clear();
    }

    pub fn simulate_pod_runtime(
        &mut self,
        event_time: f64,
        pod_name: String,
        pod_group: Option<String>,
        pod_duration: Option<f64>,
        usage_config: RuntimeResourcesUsageModelConfig,
    ) {
        let mut event_id: Option<EventId> = None;

        if pod_duration.is_some() {
            let duration = pod_duration.unwrap();
            // not long running service, schedule its finish in pod duration
            let delay = duration
                + self
                    .runtime
                    .as_ref()
                    .unwrap()
                    .config
                    .as_to_node_network_delay;

            event_id = Some(self.ctx.emit_self(
                PodFinishedRunning {
                    pod_name: pod_name.clone(),
                    node_name: self.runtime.as_ref().unwrap().node.metadata.name.clone(),
                    finish_time: event_time + duration,
                    finish_result: PodConditionType::PodSucceeded,
                },
                delay,
            ));
        }

        let mut cpu_usage_model = None;
        let mut ram_usage_model = None;

        if let Some(cpu_config) = usage_config.cpu_config {
            cpu_usage_model = Some(resource_usage_model_from_config(cpu_config, event_time));
        }
        if let Some(ram_config) = usage_config.ram_config {
            ram_usage_model = Some(resource_usage_model_from_config(ram_config, event_time));
        }

        let running_pod_info = RunningPodInfo {
            event_id,
            cpu_usage_model,
            ram_usage_model,
            pod_group,
        };

        self.running_pods.insert(pod_name, running_pod_info);
    }
}

impl EventHandler for NodeComponent {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            BindPodToNodeRequest {
                pod_name,
                pod_group,
                node_name,
                pod_duration,
                resources_usage_model_config,
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

                self.simulate_pod_runtime(
                    event.time,
                    pod_name.clone(),
                    pod_group,
                    pod_duration,
                    resources_usage_model_config,
                );

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
            PodFinishedRunning {
                pod_name,
                node_name,
                finish_time,
                finish_result,
            } => {
                self.running_pods.remove(&pod_name).unwrap();

                self.ctx.emit_now(
                    PodFinishedRunning {
                        pod_name,
                        node_name,
                        finish_time,
                        finish_result,
                    },
                    self.runtime.as_ref().unwrap().api_server,
                );
            }
            RemoveNodeRequest { node_name } => {
                assert_eq!(
                    node_name,
                    self.node_name(),
                    "Trying to remove other node than self: {:?} vs {:?}",
                    node_name,
                    self.node_name()
                );
                // Here we should cancel all events which have been already submitted to simulation
                // queue as running events as we terminate.
                self.cancel_all_running_pods();

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

                self.removed = true;
                self.removal_time = event.time;
            }
            RemovePodRequest { pod_name } => {
                if self.running_pods.contains_key(&pod_name) {
                    // pod is still running - cancel it and send response to api server about removal
                    let info = self.running_pods.remove(&pod_name).unwrap();
                    if let Some(event_id) = info.event_id {
                        self.ctx.cancel_event(event_id);
                    }
                    self.ctx.emit(
                        PodRemovedFromNode {
                            removed: true,
                            removal_time: event.time,
                            pod_name,
                        },
                        self.runtime.as_ref().unwrap().api_server,
                        self.runtime
                            .as_ref()
                            .unwrap()
                            .config
                            .as_to_node_network_delay,
                    );
                    return;
                }

                if self.canceled_pods.contains(&pod_name) {
                    // pod is already canceled due to node removal - consider it as removed at time
                    // of node removal.
                    self.ctx.emit(
                        PodRemovedFromNode {
                            removed: true,
                            removal_time: self.removal_time,
                            pod_name,
                        },
                        self.runtime.as_ref().unwrap().api_server,
                        self.runtime
                            .as_ref()
                            .unwrap()
                            .config
                            .as_to_node_network_delay,
                    );
                    return;
                }

                // otherwise, pod finished running earlier than pod removed request at node component
                // consider it as not removed
                self.ctx.emit(
                    PodRemovedFromNode {
                        removed: false,
                        removal_time: 0.0,
                        pod_name,
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
