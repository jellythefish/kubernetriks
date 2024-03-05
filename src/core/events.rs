//! Events which are emitted with different simulation components' handlers and semantically
//! are `SimulationEvent`s defined in trace interface.

use serde::Serialize;

use crate::core::node::Node;
use crate::core::pod::Pod;

#[derive(Serialize, Clone)]
pub struct CreateNodeRequest {
    pub node: Node,
}

#[derive(Serialize, Clone)]
pub struct RemoveNodeRequest {
    pub node_id: u64,
}

#[derive(Serialize, Clone)]
pub struct CreatePodRequest {
    pub pod: Pod,
}

#[derive(Serialize, Clone)]
pub struct RemovePodRequest {
    pub pod_id: u64,
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
