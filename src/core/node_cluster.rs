//! Implementation of node cluster.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use dslab_core::{cast, Event, EventHandler, SimulationContext};

use crate::core::common::{RuntimeResources, SimComponentId};
use crate::core::events::{
    BindPodToNodeRequest, CreateNodeRequest, NodeAddedToTheCluster, PodStartedRunning,
};
use crate::core::node::Node;
use crate::simulator::SimulatorConfig;

use super::events::PodFinishedRunning;
use super::pod::PodConditionType;

pub struct NodeInfo {
    allocatable: RuntimeResources,
    capacity: RuntimeResources,
    running_pods: HashSet<String>,
}

pub struct NodeCluster {
    api_server: SimComponentId,
    ctx: SimulationContext,
    // <Node name, NodeInfo> pairs
    nodes: HashMap<String, NodeInfo>,
    config: Rc<SimulatorConfig>,
}

impl NodeCluster {
    pub fn new(
        api_server: SimComponentId,
        ctx: SimulationContext,
        config: Rc<SimulatorConfig>,
    ) -> Self {
        Self {
            api_server,
            ctx,
            nodes: Default::default(),
            config,
        }
    }

    pub fn add_node(&mut self, node: Node) {
        self.nodes.insert(
            node.metadata.name,
            NodeInfo {
                allocatable: node.status.capacity.clone(),
                capacity: node.status.capacity,
                running_pods: Default::default(),
            },
        );
    }

    pub fn simulate_pod_runtime(
        &mut self,
        event_time: f64,
        pod_name: String,
        pod_duration: f64,
        node_name: String,
    ) {
        let node = self.nodes.get_mut(&node_name).unwrap();
        node.running_pods.insert(pod_name.clone());
        self.ctx.emit_self(
            PodFinishedRunning {
                pod_name,
                finish_time: event_time + pod_duration,
                finish_result: PodConditionType::PodSucceeded,
                node_name,
            },
            pod_duration,
        );
    }
}

impl EventHandler for NodeCluster {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            CreateNodeRequest { node } => {
                let node_name = node.metadata.name.clone();
                self.add_node(node);
                self.ctx.emit(
                    // TODO: ? simulate delay for node creation, event_time = event.time + delta
                    NodeAddedToTheCluster {
                        event_time: event.time,
                        node_name,
                    },
                    self.api_server,
                    self.config.as_to_nc_network_delay,
                );
            }
            BindPodToNodeRequest {
                pod_name,
                pod_duration,
                node_name,
            } => {
                self.simulate_pod_runtime(event.time, pod_name.clone(), pod_duration, node_name);
                self.ctx.emit(
                    PodStartedRunning {
                        start_time: event.time,
                        pod_name,
                    },
                    self.api_server,
                    self.config.as_to_nc_network_delay,
                );
            }
            PodFinishedRunning {
                finish_time,
                finish_result,
                pod_name,
                node_name,
            } => {
                let node = self.nodes.get_mut(&node_name).unwrap();
                node.running_pods.remove(&pod_name);
                // Redirect to api server
                self.ctx.emit(
                    PodFinishedRunning {
                        finish_time,
                        finish_result,
                        pod_name,
                        node_name,
                    },
                    self.api_server,
                    self.config.as_to_nc_network_delay,
                );
            }
        });
    }
}
