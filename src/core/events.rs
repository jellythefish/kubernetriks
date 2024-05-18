//! Events which are emitted with different simulation components' handlers and semantically
//! are `SimulationEvent`s defined in `common.rs`.

extern crate self as dslab_kubernetriks;

use dslab_kubernetriks_derive::IsSimulationEvent;

use serde::Serialize;

use crate::autoscalers::cluster_autoscaler::interface::{
    AutoscaleInfoRequestType, ScaleDownInfo, ScaleUpInfo,
};
use crate::autoscalers::horizontal_pod_autoscaler::interface::{PodGroup, PodGroupInfo};
use crate::core::node::Node;
use crate::core::pod::{Pod, PodConditionType};

use crate::core::common::RuntimeResourcesUsageModelConfig;

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

/// Event from api server to persistent storage to inform that node is created and added to the cluster.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct NodeAddedToCluster {
    pub add_time: f64,
    pub node_name: String,
}

/// Event from client or cluster autoscaler to api server to inform that node should be removed from
/// a cluster due to general reasons such as scaling down or maintenance. Api server redirects this
/// event to persistent storage
/// Does not reflect node failures.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RemoveNodeRequest {
    pub node_name: String,
}

/// Event from persistent storage to api server telling that information about requested node has
/// been persisted, so api server can remove a node.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RemoveNodeResponse {
    pub node_name: String,
}

/// Event from api server to persistent storage to inform that node is removed from the cluster.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct NodeRemovedFromCluster {
    pub removal_time: f64,
    pub node_name: String,
}

/// Event from persistent storage to scheduler to inform that node is removed from cluster and scheduler
/// should update its cache.
/// All pods that run on that node would be rescheduled.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RemoveNodeFromCache {
    pub node_name: String,
}

/// Event from client or pod autoscaler to api server with request to create a pod. Api server
/// redirects this request to persistent storage and persistent storage sends schedule request
/// to scheduler.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct CreatePodRequest {
    pub pod: Pod,
}

/// Event from client or pod autoscaler->api server to terminate pod due to general reasons such as
/// scaling down or terminating pods with infinite running duration (long running services).
/// Pod finishes its execution on node or is removed from scheduling queues. Api server redirects
/// this event to persistent storage.
/// Does not reflect pod failures.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RemovePodRequest {
    pub pod_name: String,
}

/// Event from persistent storage to api server telling that information about requested pod has
/// been persisted. If some node was assigned to pod, then terminate pod on the node.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RemovePodResponse {
    pub assigned_node: Option<String>,
    pub pod_name: String,
}

/// Event from api server to persistent storage to inform that pod is removed from the node.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct PodRemovedFromNode {
    /// Flag that is true if pod was removed from node via node removal or pod remove request
    /// and false if pod was not removed due to finished running earlier.
    pub removed: bool,
    pub removal_time: f64,
    pub pod_name: String,
}

/// Event from persistent storage to scheduler to inform that pod is removed from persistent storage
/// and probably node. So pod should be removed from scheduler cache from queues or from assignments.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RemovePodFromCache {
    pub pod_name: String,
}

// Event from persistent storage to scheduler to inform that new pod is created and ready for scheduling.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct PodScheduleRequest {
    pub pod: Pod,
}

// Event from persistent storage to scheduler to inform that new node is created and ready for scheduling.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct AddNodeToCache {
    pub node: Node,
}

// Event to inform that new pod should be bind to a node.
// Might be from scheduler to api server, from api server to persistent storage.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct AssignPodToNodeRequest {
    pub assign_time: f64,
    pub pod_name: String,
    pub node_name: String,
}

/// Event from persistent storage to api server to inform that new pod assignment is persisted and that
/// it can be bind to a node.
/// Also used as event from api server to scheduler to inform about assignment error in `assigned` flag.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct AssignPodToNodeResponse {
    pub pod_name: String,
    pub pod_group: Option<String>,
    pub pod_group_creation_time: Option<String>,
    pub node_name: String,
    pub pod_duration: Option<f64>,
    pub resources_usage_model_config: RuntimeResourcesUsageModelConfig,
}

/// Event from scheduler -> api server -> persistent storage to inform that pod cannot be scheduled
/// temporary.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct PodNotScheduled {
    pub not_scheduled_time: f64,
    pub pod_name: String,
}

// Event from api server to node cluster to inform that new pod can be started (bind).
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct BindPodToNodeRequest {
    pub pod_name: String,
    pub pod_group: Option<String>,
    pub pod_group_creation_time: Option<String>,
    pub node_name: String,
    pub pod_duration: Option<f64>,
    pub resources_usage_model_config: RuntimeResourcesUsageModelConfig,
}

// Event from to node cluster to api server to inform that new pod has started.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct BindPodToNodeResponse {
    pub pod_name: String,
    pub pod_duration: Option<f64>,
    pub node_name: String,
}

// Event from node cluster to api server and from api server to persistent storage to inform that
// pod started running on a node.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct PodStartedRunning {
    pub pod_name: String,
    pub start_time: f64,
}

// Event from node component->api server->persistent storage to inform that pod is finished.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct PodFinishedRunning {
    pub pod_name: String,
    pub node_name: String,
    pub finish_time: f64,
    pub finish_result: PodConditionType, // either PodSucceeded or PodFailed
}

/// Event from node client->api server to inform that new group of long running
/// pods with infinite duration is created.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct CreatePodGroupRequest {
    pub pod_group: PodGroup,
}

/// Event from api server to horizontal pod autoscaler to inform that new pod group is created and
/// HPA should take them into consideration.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RegisterPodGroup {
    pub info: PodGroupInfo,
}

/// Event from scheduler to itself to run pod scheduling cycle.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RunSchedulingCycle {}

/// Event from cluster autoscaler to itself to simulate working interval.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RunClusterAutoscalerCycle {}

/// Event from horizontal pod autoscaler to itself to simulate working interval.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RunHorizontalPodAutoscalerCycle {}

/// Event from metrics collector to itself to collect pod metrics in working interval.
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct RunPodMetricsCollectionCycle {}

/// Event from cluster autoscaler->api server->persistent storage to find out information
/// which cluster autoscaler needs (described in `AutoscaleInfoRequestType` enum)
#[derive(Serialize, Clone, IsSimulationEvent)]
pub struct ClusterAutoscalerRequest {
    pub request_type: AutoscaleInfoRequestType,
}

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
