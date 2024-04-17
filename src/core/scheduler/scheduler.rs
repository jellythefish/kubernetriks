//! Implementation of scheduler component which is responsible for scheduling pods for nodes.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Instant;

use dslab_core::{cast, log_debug, Event, EventHandler, SimulationContext};

use crate::core::common::{ObjectsInfo, SimComponentId};
use crate::core::events::{
    AddNodeToCacheRequest, AssignPodToNodeRequest, PodFinishedRunning, PodScheduleRequest,
    RunSchedulingCycle,
};
use crate::core::node::Node;
use crate::core::pod::Pod;
use crate::core::scheduler::interface::{PodSchedulingAlgorithm, ScheduleError};

use crate::metrics::collector::MetricsCollector;

use crate::simulator::SimulationConfig;

pub struct Scheduler {
    api_server: SimComponentId,

    /// Cache which is updated based on events from persistent storage
    objects_cache: ObjectsInfo,
    scheduler_algorithm: Box<dyn PodSchedulingAlgorithm>,

    /// Queue of timestamps and pod names to schedule.
    /// Pods as objects themselves are stored in objects_cache, timestamps reflects the time
    /// when pod was pushed to the queue.
    pod_queue: VecDeque<(f64, String)>,

    ctx: SimulationContext,
    config: Rc<SimulationConfig>,

    metrics_collector: Rc<RefCell<MetricsCollector>>,
}

impl Scheduler {
    pub fn new(
        api_server: SimComponentId,
        scheduler_algorithm: Box<dyn PodSchedulingAlgorithm>,
        ctx: SimulationContext,
        config: Rc<SimulationConfig>,
        metrics_collector: Rc<RefCell<MetricsCollector>>,
    ) -> Self {
        Self {
            api_server,
            objects_cache: Default::default(),
            scheduler_algorithm,
            pod_queue: Default::default(),
            ctx,
            config,
            metrics_collector,
        }
    }

    pub fn start(&mut self) {
        self.ctx.emit_self_now(RunSchedulingCycle {});
    }

    pub fn add_node(&mut self, node: Node) {
        self.objects_cache
            .nodes
            .insert(node.metadata.name.clone(), node);
    }

    pub fn add_pod(&mut self, pod: Pod) {
        self.objects_cache
            .pods
            .insert(pod.metadata.name.clone(), pod);
    }

    pub fn get_node(&self, node_name: &str) -> &Node {
        self.objects_cache.nodes.get(node_name).unwrap()
    }

    pub fn get_pod(&self, pod_name: &str) -> &Pod {
        self.objects_cache.pods.get(pod_name).unwrap()
    }

    pub fn node_count(&self) -> usize {
        self.objects_cache.nodes.len()
    }

    pub fn pod_count(&self) -> usize {
        self.objects_cache.pods.len()
    }

    pub fn set_scheduler_algorithm(
        &mut self,
        scheduler_algorithm: Box<dyn PodSchedulingAlgorithm>,
    ) {
        self.scheduler_algorithm = scheduler_algorithm
    }

    fn reserve_node_resources(&mut self, pod_name: &str, assigned_node: &str) {
        let pod = self.objects_cache.pods.get(pod_name).unwrap();
        let node = self.objects_cache.nodes.get_mut(assigned_node).unwrap();
        node.status.allocatable.cpu -= pod.spec.resources.requests.cpu;
        node.status.allocatable.ram -= pod.spec.resources.requests.ram;
    }

    fn assign_node_to_pod(&mut self, pod_name: &str, node_name: &str) {
        self.objects_cache
            .pods
            .get_mut(pod_name)
            .unwrap()
            .status
            .assigned_node = node_name.to_string();

        log_debug!(
            self.ctx,
            "Pod {:?} has been assigned a Node {:?}",
            pod_name,
            node_name,
        );
    }

    fn release_node_resources(&mut self, pod_name: &str) {
        let pod = self.objects_cache.pods.get(pod_name).unwrap();
        let node = self
            .objects_cache
            .nodes
            .get_mut(&pod.status.assigned_node)
            .unwrap();
        node.status.allocatable.cpu += pod.spec.resources.requests.cpu;
        node.status.allocatable.ram += pod.spec.resources.requests.ram;
        self.objects_cache.pods.remove(pod_name);
    }

    fn schedule_one(&self, pod: &Pod) -> Result<String, ScheduleError> {
        self.scheduler_algorithm
            .schedule_one(pod, &self.objects_cache.nodes)
    }

    fn run_scheduling_cycle(&mut self, scheduling_cycle_event_time: f64) {
        let cycle_start_time = Instant::now();
        let mut unscheduled_queue: VecDeque<(f64, String)> = Default::default();

        while let Some((ts, next_pod_name)) = self.pod_queue.pop_front() {
            let pod_queue_time = scheduling_cycle_event_time + cycle_start_time.elapsed().as_secs_f64() - ts;
            let pod_schedule_time = Instant::now();
            let assigned_node =
                match self.schedule_one(self.objects_cache.pods.get(&next_pod_name).unwrap()) {
                    Ok(assigned_node) => assigned_node,
                    Err(err) => {
                        log_debug!(
                            self.ctx,
                            "failed to schedule pod {:?}: {:?}",
                            next_pod_name,
                            err
                        );
                        unscheduled_queue.push_back((ts, next_pod_name));
                        continue;
                    }
                };

            self.reserve_node_resources(&next_pod_name, &assigned_node);
            self.assign_node_to_pod(&next_pod_name, &assigned_node);

            self.ctx.emit(
                AssignPodToNodeRequest {
                    pod_name: next_pod_name,
                    node_name: assigned_node,
                },
                self.api_server,
                cycle_start_time.elapsed().as_secs_f64() + self.config.sched_to_as_network_delay,
            );

            self.metrics_collector.borrow_mut().increment_pod_schedule_time(pod_schedule_time.elapsed().as_secs_f64());
            self.metrics_collector.borrow_mut().increment_pod_queue_time(pod_queue_time);
        }

        assert_eq!(
            0,
            self.pod_queue.len(),
            "impossible scenario: queue is not empty after while cycle"
        );
        self.pod_queue = unscheduled_queue;

        let elapsed = cycle_start_time.elapsed();
        let next_cycle_delay =
            f64::max(elapsed.as_secs_f64(), self.config.scheduling_cycle_interval);

        self.ctx.emit_self(RunSchedulingCycle {}, next_cycle_delay);
    }
}

impl EventHandler for Scheduler {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            RunSchedulingCycle {} => {
                self.run_scheduling_cycle(event.time);
            }
            AddNodeToCacheRequest { node } => {
                self.add_node(node);
            }
            PodScheduleRequest { pod } => {
                let pod_name = pod.metadata.name.clone();
                self.pod_queue.push_back((event.time, pod_name));
                self.add_pod(pod);
            }
            PodFinishedRunning {
                finish_time: _,
                finish_result: _,
                pod_name,
            } => {
                self.release_node_resources(&pod_name);
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use dslab_core::Simulation;

    use crate::core::node::Node;
    use crate::core::pod::Pod;
    use crate::core::scheduler::interface::ScheduleError;
    use crate::core::scheduler::kube_scheduler::{default_kube_scheduler_config, KubeScheduler};
    use crate::core::scheduler::scheduler::Scheduler;
    
    use crate::metrics::collector::MetricsCollector;

    use crate::test_util::helpers::default_test_simulation_config;

    fn create_scheduler() -> Scheduler {
        let mut fake_sim = Simulation::new(0);

        Scheduler::new(
            0,
            Box::new(KubeScheduler {
                config: default_kube_scheduler_config(),
            }),
            fake_sim.create_context("scheduler"),
            Rc::new(default_test_simulation_config()),
            Rc::new(RefCell::new(MetricsCollector::new())),
        )
    }

    fn register_nodes(scheduler: &mut Scheduler, nodes: Vec<Node>) {
        for node in nodes.into_iter() {
            scheduler.add_node(node);
        }
    }

    fn register_pods(scheduler: &mut Scheduler, pods: Vec<Pod>) {
        for pod in pods.into_iter() {
            scheduler.add_pod(pod);
        }
    }

    #[test]
    fn test_no_nodes_no_schedule() {
        let scheduler = create_scheduler();
        let pod = Pod::new("pod_1".to_string(), 4000, 16000, 5.0);
        assert_eq!(
            scheduler.schedule_one(&pod).err().unwrap(),
            ScheduleError::NoNodesInCluster
        );
    }

    #[test]
    fn test_pod_has_requested_zero_resources() {
        let mut scheduler = create_scheduler();
        let pod = Pod::new("pod_1".to_string(), 0, 0, 5.0);
        let node = Node::new("node1".to_string(), 3000, 8589934592);
        register_nodes(&mut scheduler, vec![node]);
        assert_eq!(
            scheduler.schedule_one(&pod).err().unwrap(),
            ScheduleError::RequestedResourcesAreZeros
        );
    }

    #[test]
    fn test_no_sufficient_nodes_for_scheduling() {
        let mut scheduler = create_scheduler();
        let pod = Pod::new("pod_1".to_string(), 6000, 12884901888, 5.0);
        let node = Node::new("node1".to_string(), 3000, 8589934592);
        register_nodes(&mut scheduler, vec![node]);
        assert_eq!(
            scheduler.schedule_one(&pod).err().unwrap(),
            ScheduleError::NoSufficientResources
        );
    }

    #[test]
    fn test_correct_pod_scheduling() {
        let _ = env_logger::try_init();

        let mut scheduler = create_scheduler();
        let pod = Pod::new("pod_1".to_string(), 6000, 12884901888, 5.0);
        let node1 = Node::new("node1".to_string(), 8000, 14589934592);
        let node2 = Node::new("node2".to_string(), 7000, 20589934592);
        let node3 = Node::new("node3".to_string(), 6000, 100589934592);
        // scores
        // node1: ((8000 - 6000) * 100 / 8000 + (14589934592 - 12884901888) * 100 / 14589934592) / 2 = 18.34
        // node2: ((7000 - 6000) * 100 / 7000 + (20589934592 - 12884901888) * 100 / 20589934592) / 2 = 25.85
        // node3: ((6000 - 6000) * 100 / 6000 + (100589934592 - 12884901888) * 100 / 100589934592) / 2 = 43.59
        // node3 - max score - choose it for scheduling
        register_nodes(&mut scheduler, vec![node1, node2, node3.clone()]);
        assert_eq!(
            *scheduler.schedule_one(&pod).ok().unwrap(),
            node3.metadata.name
        );
    }

    #[test]
    fn test_several_pod_scheduling() {
        let mut scheduler = create_scheduler();
        let node_name = "node1";
        let pod1 = Pod::new("pod_1".to_string(), 4000, 8589934592, 5.0);
        let pod2 = Pod::new("pod_2".to_string(), 2000, 4294967296, 5.0);
        let pod3 = Pod::new("pod_3".to_string(), 8000, 8589934592, 5.0);
        let pod4 = Pod::new("pod_4".to_string(), 10000, 8589934592, 5.0);
        let node1 = Node::new(node_name.to_string(), 16000, 100589934592);
        register_nodes(&mut scheduler, vec![node1.clone()]);
        register_pods(
            &mut scheduler,
            vec![pod1.clone(), pod2.clone(), pod3.clone(), pod4.clone()],
        );
        assert_eq!(&*scheduler.schedule_one(&pod1).ok().unwrap(), node_name);
        scheduler.reserve_node_resources(&pod1.metadata.name, node_name);
        assert_eq!(&*scheduler.schedule_one(&pod2).ok().unwrap(), node_name);
        scheduler.reserve_node_resources(&pod2.metadata.name, node_name);
        assert_eq!(&*scheduler.schedule_one(&pod3).ok().unwrap(), node_name);
        scheduler.reserve_node_resources(&pod3.metadata.name, node_name);
        // there is no place left on node for the fourth pod
        assert_eq!(
            scheduler.schedule_one(&pod4).err().unwrap(),
            ScheduleError::NoSufficientResources
        );
    }
}
