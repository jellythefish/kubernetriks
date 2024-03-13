//! Implementation of persistent storage for saving all information about kubernetes objects.
//! In k8s etcd plays this role, in our simulator it is a component which implements simple
//! in-memory key-value storage.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use dslab_core::{cast, Event, EventHandler, SimulationContext};
use log::debug;

use crate::core::common::SimComponentId;
use crate::core::events::{CreateNodeRequest, CreateNodeResponse, NodeAddedToTheCluster};
use crate::core::node::{Node, NodeConditionType};
use crate::core::pod::Pod;
use crate::simulator::SimulatorConfig;

pub struct StorageData {
    // State about current nodes of a cluster: <Node name, Node>
    pub nodes: HashMap<String, Node>,
    // State about current pods of a cluster: <Pod name, Pod>
    pub pods: HashMap<String, Pod>,
}

pub struct PersistentStorage {
    // Identifier of persistent storage as a simulation component.
    api_server: SimComponentId,
    scheduler: SimComponentId,

    storage_data: Rc<RefCell<StorageData>>,

    ctx: SimulationContext,
    config: Rc<SimulatorConfig>,
}

impl PersistentStorage {
    pub fn new(
        api_server_id: SimComponentId,
        scheduler_id: SimComponentId,
        storage_data: Rc<RefCell<StorageData>>,
        ctx: SimulationContext,
        config: Rc<SimulatorConfig>,
    ) -> Self {
        Self {
            api_server: api_server_id,
            scheduler: scheduler_id,
            storage_data,
            ctx,
            config,
        }
    }
}

impl EventHandler for PersistentStorage {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            CreateNodeRequest { node } => {
                let node_name = node.metadata.name.clone();
                self.storage_data
                    .borrow_mut()
                    .nodes
                    .insert(node_name.clone(), node);
                self.ctx.emit(
                    CreateNodeResponse {
                        created: true,
                        node_name,
                    },
                    self.api_server,
                    self.config.as_to_ps_network_delay,
                );
            }
            NodeAddedToTheCluster {
                event_time,
                node_name,
            } => {
                self.storage_data
                    .borrow_mut()
                    .nodes
                    .get_mut(&node_name)
                    .unwrap()
                    .update_node_condition(
                        "True".to_string(),
                        NodeConditionType::NodeCreated,
                        event_time,
                    );
                debug!(
                    "Updated node conditions: {:?}",
                    self.storage_data.borrow_mut().nodes[&node_name]
                        .status
                        .conditions
                );
            }
        })
    }
}
