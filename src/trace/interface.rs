// Interface for any trace which serves as input to the simulator.

use dslab_core::event::EventData;

// SimulationEvent aliases to the `EventData` which is a trait which all simulation events in dslab
// framework implement. In out simulator we explicitly use `SimulationEvent` to differ trace's events
// from events which are created by kubernetes components.
// So any event which implements `SimulationEvent` becomes emittable via `SimulationContext` from dslab.
pub type SimulationEvent = Box<dyn EventData>;

// And we define trait Trace to represent any trace format acceptable by simulator.
pub trait Trace {
    // Any Trace should implement this method to convert arbitrary format of trace events to the
    // format of events which are emitted by simulator components defined in core::events.
    // First element in the tuple is timestamp, second - event.
    fn convert_to_simulator_events(&mut self) -> Vec<(u64, SimulationEvent)>;
}
