//! Events which are emitted with different simulation components' handlers and semantically
//! are `SimulationEvent`s defined in trace interface.

use serde::Serialize;

use crate::core::node::Node;
use crate::core::pod::Pod;

use super::pod::PodConditionType;

// Client to kube-api-server events

// K8s supports two two main ways to have Nodes added to the API server:
// 1) The kubelet on a node self-registers to the control plane.
// 2) A user manually adds a Node object.
//
// In our simulator we implement the second approach considering that instead of a user we have node
// creation events in the trace.

#[derive(Serialize, Clone)]
pub struct CreateNodeRequest {
    pub node: Node,
}

#[derive(Serialize, Clone)]
pub struct CreateNodeResponse {
    pub created: bool,
    pub node_name: String,
}

#[derive(Serialize, Clone)]
pub struct RemoveNodeRequest {
    pub node_name: String,
}

#[derive(Serialize, Clone)]
pub struct CreatePodRequest {
    pub pod: Pod,
}

#[derive(Serialize, Clone)]
pub struct RemovePodRequest {
    pub pod_name: String,
}

// Event from api server to persistent storage to tell that node is created in cluster.
#[derive(Serialize, Clone)]
pub struct NodeAddedToTheCluster {
    pub event_time: f64,
    pub node_name: String,
}

// Event from persistent storage to scheduler to tell that new pod is created and ready for scheduling.
#[derive(Serialize, Clone)]
pub struct PodCreated {
    pub pod_name: String,
}

// Event to tell that new pod should be bind to a node.
// Might be from scheduler to api server, from api server to persistent storage.
#[derive(Serialize, Clone)]
pub struct AssignPodToNodeRequest {
    pub pod_name: String,
    pub node_name: String,
}

// Event from persistent storage to api server to tell that new pod assignment is persisted and that
// it can be bind to a node.
#[derive(Serialize, Clone)]
pub struct AssignPodToNodeResponse {
    pub pod_name: String,
    pub pod_duration: f64,
    pub node_name: String,
}

// Event from api server to node cluster to tell that new pod can be started (bind).
#[derive(Serialize, Clone)]
pub struct BindPodToNodeRequest {
    pub pod_name: String,
    pub pod_duration: f64,
    pub node_name: String,
}

// Event from to node cluster to api server to tell that new pod has started.
#[derive(Serialize, Clone)]
pub struct BindPodToNodeResponse {
    pub pod_name: String,
    pub pod_duration: f64,
    pub node_name: String,
}

// Event from node cluster to api server and from api server to persistent storage to tell that
// pod started running on a node.
#[derive(Serialize, Clone)]
pub struct PodStartedRunning {
    pub start_time: f64,
    pub pod_name: String,
}

// Event from node cluster to self to simulate pod running, also from node cluster to api server
// and from api server to persistent storage to tell that pod is finished.
#[derive(Serialize, Clone)]
pub struct PodFinishedRunning {
    pub finish_time: f64,
    pub finish_result: PodConditionType, // either PodSucceeded or PodFailed
    pub pod_name: String,
    pub node_name: String,
}

// Potential macro to be in dslab_core
// Expect event.data to be Box<Box<dyn EventData>> to downcast it first and then extract a real type.
#[macro_export]
macro_rules! cast_box {
    ( match $event:ident.data { $( $type:ident { $($tt:tt)* } => { $($expr:tt)* } )+ } ) => {
        if let Ok(boxed_event) = $event.data.clone().downcast::<Box<dyn dslab_core::event::EventData>>() {
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
            // Fallback to normal cast! if data.event is already Box<dyn EventData>.
            dslab_core::cast!(match $event.data { $( $type { $($tt)* } => { $($expr)* } )+ } );
        }
    }
}
