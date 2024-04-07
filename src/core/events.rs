//! Events which are emitted with different simulation components' handlers and semantically
//! are `SimulationEvent`s defined in `common.rs`.

extern crate self as dslab_kubernetriks;
use dslab_kubernetriks_derive::IsSimulationEvent;

use serde::Serialize;

use crate::core::node::Node;
use crate::core::pod::{Pod, PodConditionType};

// Client to kube-api-server events

// K8s supports two two main ways to have Nodes added to the API server:
// 1) The kubelet on a node self-registers to the control plane.
// 2) A user manually adds a Node object.
//
// In our simulator we implement the second approach considering that instead of a user we have node
// creation events in the node trace.

#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct CreateNodeRequest {
    pub node: Node,
}

#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct CreateNodeResponse {
    pub created: bool,
    pub node_name: String,
}

#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RemoveNodeRequest {
    pub node_name: String,
}

#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct CreatePodRequest {
    pub pod: Pod,
}

#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RemovePodRequest {
    pub pod_name: String,
}

// Event from api server to persistent storage to tell that node is created in cluster.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct NodeAddedToTheCluster {
    pub event_time: f64,
    pub node_name: String,
}

// Event from persistent storage to scheduler to tell that new pod is created and ready for scheduling.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct PodScheduleRequest {
    pub pod: Pod,
}

// Event from persistent storage to scheduler to tell that new node is created and ready for scheduling.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct AddNodeToCacheRequest {
    pub node: Node,
}

// Event to tell that new pod should be bind to a node.
// Might be from scheduler to api server, from api server to persistent storage.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct AssignPodToNodeRequest {
    pub pod_name: String,
    pub node_name: String,
}

// Event from persistent storage to api server to tell that new pod assignment is persisted and that
// it can be bind to a node.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct AssignPodToNodeResponse {
    pub pod_name: String,
    pub pod_duration: f64,
    pub node_name: String,
}

// Event from api server to node cluster to tell that new pod can be started (bind).
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct BindPodToNodeRequest {
    pub pod_name: String,
    pub pod_duration: f64,
    pub node_name: String,
}

// Event from to node cluster to api server to tell that new pod has started.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct BindPodToNodeResponse {
    pub pod_name: String,
    pub pod_duration: f64,
    pub node_name: String,
}

// Event from node cluster to api server and from api server to persistent storage to tell that
// pod started running on a node.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct PodStartedRunning {
    pub pod_name: String,
    pub start_time: f64,
}

// Event from node cluster to self to simulate pod running, also from node cluster to api server
// and from api server to persistent storage to tell that pod is finished.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct PodFinishedRunning {
    pub pod_name: String,
    pub finish_time: f64,
    pub finish_result: PodConditionType, // either PodSucceeded or PodFailed
}

// Event from scheduler to itself to run pod scheduling cycle.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RunSchedulingCycle {}

// Expect event.data to be Box<Box<dyn SimulationEvent>> to downcast it first and then extract a real type.
#[macro_export]
macro_rules! cast_box {
    ( match $event:ident.data { $( $type:ident { $($tt:tt)* } => { $($expr:tt)* } )+ } ) => {
        if let Ok(boxed_event) = $event.data.clone().downcast::<Box<dyn $crate::core::common::SimulationEvent>>() {
            $(
                if boxed_event.is::<$type>() {
                    if let Ok(__value) = boxed_event.downcast::<$type>() {
                        let $type { $($tt)* } = *__value;
                        $($expr)*
                    }
                } else
            )*
            {
                // potential $crate::log::log_unhandled_event($event);
                log::error!("unhandled event: {:?}", serde_type_name::type_name(&boxed_event).unwrap());
            }
        } else {
            // Fallback to normal cast! if data.event is already Box<dyn SimulationEvent>.
            dslab_core::cast!(match $event.data { $( $type { $($tt)* } => { $($expr)* } )+ } );
        }
    }
}
