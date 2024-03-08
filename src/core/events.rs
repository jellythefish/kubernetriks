//! Events which are emitted with different simulation components' handlers and semantically
//! are `SimulationEvent`s defined in trace interface.

use serde::Serialize;

use crate::core::node_info::NodeInfo;
use crate::core::node_info::{NodeId, NodeSpec};
use crate::core::pod::{PodId, PodSpec};

#[derive(Serialize, Clone)]
pub struct CreateNodeRequest {
    pub node_spec: NodeSpec,
}

#[derive(Serialize, Clone)]
pub struct RemoveNodeRequest {
    pub node_id: NodeId,
}

#[derive(Serialize, Clone)]
pub struct CreatePodRequest {
    pub pod_spec: PodSpec,
}

#[derive(Serialize, Clone)]
pub struct RemovePodRequest {
    pub pod_id: PodId,
}

#[derive(Serialize, Clone)]
pub struct AddNodeToClusterRequest {
    pub node_id: NodeId,
}

#[derive(Serialize, Clone)]
pub struct NodeAddedToCluster {
    pub node_info: NodeInfo,
}

// Potential macro to be in dslab_core
// Expect event.data to be Box<dyn EventData> to downcast it first and then extract a real type.
#[macro_export]
macro_rules! cast_box {
    ( match $event:ident.data { $( $type:ident { $($tt:tt)* } => { $($expr:tt)* } )+ } ) => {
        if let Ok(boxed_event) = $event.data.downcast::<Box<dyn dslab_core::event::EventData>>() {
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
            log::error!("event.data must be Box<dyn EventData> to cast_box, but it's not");
        }
    }
}
