//! Node component simulates a real node running pods.

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use dslab_core::{cast, Event, EventHandler, SimulationContext};

use crate::core::events::{BindPodToNodeRequest, PodFinishedRunning, PodStartedRunning};
use crate::core::node::Node;

use crate::core::pod::PodConditionType;

use crate::core::common::SimComponentId;
use crate::simulator::SimulatorConfig;

pub struct NodeComponent {
    pub node: Node,

    pub running_pods: HashSet<String>,

    pub api_server: Option<SimComponentId>,

    ctx: SimulationContext,
    pub config: Option<Rc<SimulatorConfig>>,
}

impl NodeComponent {
    pub fn new(ctx: SimulationContext) -> Self {
        Self {
            node: Default::default(),
            running_pods: Default::default(),
            api_server: None,
            ctx,
            config: None,
        }
    }

    pub fn clear_state(&mut self) {
        self.node = Default::default();
        self.running_pods.clear();
    }

    pub fn simulate_pod_runtime(
        &mut self,
        event_time: f64,
        pod_name: String,
        pod_duration: f64,
    ) {
        self.running_pods.insert(pod_name.clone());
        self.ctx.emit_self(
            PodFinishedRunning {
                pod_name,
                finish_time: event_time + pod_duration,
                finish_result: PodConditionType::PodSucceeded,
            },
            pod_duration,
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
                if node_name != self.node.metadata.name {
                    panic!("Pod is assigned to node with different node name: pod - {:?}, current node - {:?}, assigned node - {:?}", pod_name, self.node.metadata.name, node_name);
                }
                self.simulate_pod_runtime(event.time, pod_name.clone(), pod_duration);
                self.ctx.emit(
                    PodStartedRunning {
                        start_time: event.time,
                        pod_name,
                    },
                    self.api_server.unwrap(),
                    self.config.as_ref().unwrap().as_to_node_network_delay,
                );
            }
            PodFinishedRunning {
                finish_time,
                finish_result,
                pod_name,
            } => {
                self.running_pods.remove(&pod_name);
                // Redirect to api server
                self.ctx.emit(
                    PodFinishedRunning {
                        finish_time,
                        finish_result,
                        pod_name,
                    },
                    self.api_server.unwrap(),
                    self.config.as_ref().unwrap().as_to_node_network_delay,
                );
            }
        });
    }
}
