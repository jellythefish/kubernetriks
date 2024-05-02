//! Events which are emitted with different simulation components' handlers and semantically
//! are `SimulationEvent`s defined in `common.rs`.

extern crate self as dslab_kubernetriks;
use dslab_kubernetriks_derive::IsSimulationEvent;

use serde::Serialize;

use crate::autoscaler::cluster_autoscaler::{ScaleDownInfo, ScaleUpInfo};

use crate::core::node::Node;
use crate::core::pod::{Pod, PodConditionType};

// K8s supports two two main ways to have Nodes added to the API server:
// 1) The kubelet on a node self-registers to the control plane.
// 2) A user manually adds a Node object.
//
// In our simulator we implement the second approach considering that instead of a user we have node
// creation events in the node trace.

/// Event from client to api server with request to create node. Api server redirects this request
/// firstly to persistent storage and on response creates a node.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct CreateNodeRequest {
    pub node: Node,
}

/// Event from persistent storage to api server telling that information about requested node has
/// been persisted, so api server can create a node.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct CreateNodeResponse {
    pub node_name: String,
}

/// Event from api server to persistent storage to tell that node is created and added to the cluster.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct NodeAddedToCluster {
    pub add_time: f64,
    pub node_name: String,
}

/// Event from client or cluster autoscaler to api server to tell that node should be removed from
/// a cluster due to general reasons such as scaling down or maintenance. Api server redirects this
/// event to persistent storage 
/// Does not reflect node failures.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RemoveNodeRequest {
    pub node_name: String,
}

/// Event from api server to persistent storage to tell that node is removed from the cluster.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RemoveNodeResponse {
    pub node_name: String,
}

/// Event from persistent storage to api server telling that information about requested node has
/// been persisted, so api server can remove a node.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct NodeRemovedFromCluster {
    pub removal_time: f64,
    pub node_name: String,
}

/// Event from persistent storage to scheduler to tell that node is removed from cluster.
/// So all pods that run on that node should be rescheduled.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RemoveNodeFromCache {
    pub node_name: String,
}

/// Event from node component to api server that node has failed. Such event implements mechanism
/// to monitor node failures instead of standalone node health monitor component for simplicity.
/// All pods that ran on this node should be rescheduled.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct NodeFailed {
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

// Event from persistent storage to scheduler to tell that new pod is created and ready for scheduling.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct PodScheduleRequest {
    pub pod: Pod,
}

// Event from persistent storage to scheduler to tell that new node is created and ready for scheduling.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct AddNodeToCache {
    pub node: Node,
}

// Event to tell that new pod should be bind to a node.
// Might be from scheduler to api server, from api server to persistent storage.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct AssignPodToNodeRequest {
    pub assign_time: f64,
    pub pod_name: String,
    pub node_name: String,
}

/// Event from persistent storage to api server to tell that new pod assignment is persisted and that
/// it can be bind to a node.
/// Also used as event from api server to scheduler to tell about assignment error in `assigned` flag.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct AssignPodToNodeResponse {
    pub pod_name: String,
    pub pod_duration: f64,
    pub node_name: String,
}

/// Event from scheduler -> api server -> persistent storage to tell that pod cannot be scheduled
/// temporary.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct PodNotScheduled {
    pub not_scheduled_time: f64,
    pub pod_name: String,
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

// Event from node component->api server->persistent storage to tell that pod is finished.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct PodFinishedRunning {
    pub pod_name: String,
    pub node_name: String,
    pub finish_time: f64,
    pub finish_result: PodConditionType, // either PodSucceeded or PodFailed
}

/// Event from scheduler to itself to run pod scheduling cycle.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RunSchedulingCycle {}

/// Event from cluster autoscaler to itself to simulate working interval.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RunClusterAutoscalerCycle {}

/// Event from cluster autoscaler->api server->persistent storage to find out what autoscaler
/// should do: scale up or scale down.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct ClusterAutoscalerRequest {}

/// Event from persistent storage->api server->cluster autoscaler about corresponding info to request.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct ClusterAutoscalerResponse {
    pub scale_up: Option<ScaleUpInfo>,
    pub scale_down: Option<ScaleDownInfo>,
}

/// Event from scheduler to itself to flush unschedulable queue leftover.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct FlushUnschedulableQueueLeftover {}

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
