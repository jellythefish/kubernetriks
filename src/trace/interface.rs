// Interface for any trace which serves as input to the simulator.

use crate::core::common::SimulationEvent;

// Trait to represent any trace format acceptable by simulator.
pub trait Trace {
    // Any Trace should implement this method to convert arbitrary format of trace events to the
    // format of events which are emitted by simulator components defined in core::events.
    // First element in the tuple is timestamp, second - event.
    fn convert_to_simulator_events(&mut self) -> Vec<(f64, Box<dyn SimulationEvent>)>;
}
