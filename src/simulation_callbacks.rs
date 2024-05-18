//! Simulation callbacks interface and implementations to define how simulator should behave on start,
//! step, finish and when it should stop running.

use log::info;

use crate::{metrics::printer::print_metrics, simulator::KubernetriksSimulation};

pub trait SimulationCallbacks {
    /// Runs before starting a simulation run.
    fn on_simulation_start(&mut self, _sim: &mut KubernetriksSimulation) {}

    /// Runs on each step of a simulation run, returns false if the simulation must be stopped.
    fn on_step(&mut self, _sim: &mut KubernetriksSimulation) -> bool {
        true
    }

    /// Runs upon the completion of a simulation run, returns results of this run.
    fn on_simulation_finish(&mut self, _sim: &mut KubernetriksSimulation) {}
}

pub struct RunUntilAllPodsAreFinishedCallbacks {}

/// Returns true if all pods are terminated.
fn check_all_short_pods_terminated(sim: &mut KubernetriksSimulation) -> bool {
    let terminated_pods = sim
        .metrics_collector
        .borrow()
        .metrics
        .internal
        .terminated_pods;
    let total_pods_in_trace = sim.metrics_collector.borrow().metrics.total_pods_in_trace;
    info!(
        "Processed {} out of {} pods",
        terminated_pods, total_pods_in_trace
    );

    return terminated_pods >= total_pods_in_trace;
}

fn assert_and_print(sim: &mut KubernetriksSimulation) {
    let terminated_pods = sim
        .metrics_collector
        .borrow()
        .metrics
        .internal
        .terminated_pods;

    let pods_succeeded = sim.metrics_collector.borrow().metrics.pods_succeeded;
    let pods_unschedulable = sim.metrics_collector.borrow().metrics.pods_unschedulable;
    let pods_failed = sim.metrics_collector.borrow().metrics.pods_failed;
    let pods_removed = sim.metrics_collector.borrow().metrics.pods_removed;

    assert_eq!(
        terminated_pods,
        pods_succeeded + pods_unschedulable + pods_failed + pods_removed
    );
    if !sim.config.metrics_printer.is_none() {
        print_metrics(
            sim.metrics_collector.clone(),
            sim.config.metrics_printer.as_ref().unwrap(),
        );
    };
}

impl SimulationCallbacks for RunUntilAllPodsAreFinishedCallbacks {
    fn on_step(&mut self, sim: &mut KubernetriksSimulation) -> bool {
        if sim.sim.time() % 1000.0 == 0.0 {
            return !check_all_short_pods_terminated(sim);
        }
        true
    }

    fn on_simulation_finish(&mut self, sim: &mut KubernetriksSimulation) {
        assert_and_print(sim);
    }
}

pub struct RunUntilAllPodsAreFinishedAndLongRunningPodsExceedDeadlineCallbacks {
    deadline_time: f64,
    all_short_pods_terminated: bool,
}

impl RunUntilAllPodsAreFinishedAndLongRunningPodsExceedDeadlineCallbacks {
    pub fn new(deadline_time: f64) -> Self {
        Self {
            deadline_time,
            all_short_pods_terminated: false,
        }
    }
}

impl SimulationCallbacks for RunUntilAllPodsAreFinishedAndLongRunningPodsExceedDeadlineCallbacks {
    fn on_step(&mut self, sim: &mut KubernetriksSimulation) -> bool {
        if self.all_short_pods_terminated {
            // all short pods are finished, check if we reached deadline
            return sim.sim.time() < self.deadline_time;
        }
        if sim.sim.time() % 1000.0 == 0.0 {
            self.all_short_pods_terminated = check_all_short_pods_terminated(sim);
            return !self.all_short_pods_terminated;
        }
        true
    }

    fn on_simulation_finish(&mut self, sim: &mut KubernetriksSimulation) {
        assert_and_print(sim);
    }
}
