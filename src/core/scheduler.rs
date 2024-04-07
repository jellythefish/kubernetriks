//! Implementation of kube-scheduler component which is responsible for scheduling pods for nodes.

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
use crate::simulator::SimulationConfig;

#[derive(Debug, PartialEq)]
pub enum ScheduleError {
    RequestedResourcesAreZeros,
    NoSufficientNodes,
}

pub trait AnyScheduler {
    // Method which should implement any scheduler to assign a node on which the pod will be executed.
    // Returns result which if is Ok - the name of assigned node, Err - scheduling error.
    fn schedule_one<'a>(
        &self,
        pod: &'a Pod,
        nodes: &mut dyn Iterator<Item = &'a Node>,
    ) -> Result<&'a Node, ScheduleError>;
}

pub trait KubeGenericScheduler: AnyScheduler {
    fn filter<'a>(&self, pod: &'a Pod, nodes: &mut dyn Iterator<Item = &'a Node>) -> Vec<&'a Node>;
    fn score(&self, pod: &Pod, node: &Node) -> f64;

    fn schedule_one<'a>(
        &self,
        pod: &'a Pod,
        nodes: &mut dyn Iterator<Item = &'a Node>,
    ) -> Result<&'a Node, ScheduleError> {
        let requested_resources = &pod.spec.resources.requests;
        if requested_resources.cpu == 0 && requested_resources.ram == 0 {
            return Err(ScheduleError::RequestedResourcesAreZeros);
        }

        let filtered_nodes = self.filter(pod, nodes);
        if filtered_nodes.len() == 0 {
            return Err(ScheduleError::NoSufficientNodes);
        }

        let mut assigned_node = filtered_nodes[0];
        let mut max_score = 0.0;

        for node in filtered_nodes {
            let score = self.score(pod, node);
            if score >= max_score {
                assigned_node = &node;
                max_score = score;
            }
        }
        Ok(assigned_node)
    }
}

pub struct Scheduler {
    api_server: SimComponentId,

    // Cache which is updated based on events from persistent storage
    objects_cache: ObjectsInfo,
    scheduler_impl: Box<dyn AnyScheduler>,

    // queue of pod names to schedule, pod as objects themselves are stored in objects_cache
    pod_queue: VecDeque<String>,

    ctx: SimulationContext,
    config: Rc<SimulationConfig>,
}

impl Scheduler {
    pub fn new(
        api_server: SimComponentId,
        scheduler_impl: Box<dyn AnyScheduler>,
        ctx: SimulationContext,
        config: Rc<SimulationConfig>,
    ) -> Self {
        Self {
            api_server,
            objects_cache: Default::default(),
            scheduler_impl,
            pod_queue: Default::default(),
            ctx,
            config,
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

    pub fn get_node(&self, node_name: &str) -> Node {
        self.objects_cache.nodes.get(node_name).unwrap().clone()
    }

    pub fn get_pod(&self, pod_name: &str) -> Pod {
        self.objects_cache.pods.get(pod_name).unwrap().clone()
    }

    pub fn node_count(&self) -> usize {
        self.objects_cache.nodes.len()
    }

    pub fn pod_count(&self) -> usize {
        self.objects_cache.pods.len()
    }

    pub fn set_scheduler_impl(&mut self, scheduler_impl: Box<dyn AnyScheduler>) {
        self.scheduler_impl = scheduler_impl
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

    fn schedule_one<'a>(&'a self, pod: &'a Pod) -> Result<&'a Node, ScheduleError> {
        let mut nodes_iter = self.objects_cache.nodes.values();
        self.scheduler_impl.schedule_one(pod, &mut nodes_iter)
    }

    fn run_scheduling_cycle(&mut self) {
        let cycle_start_time = Instant::now();
        let mut unscheduled_queue: VecDeque<String> = Default::default();

        while let Some(next_pod_name) = self.pod_queue.pop_front() {
            let assigned_node =
                match self.schedule_one(self.objects_cache.pods.get(&next_pod_name).unwrap()) {
                    Ok(assigned_node) => assigned_node.metadata.name.clone(),
                    Err(err) => {
                        log_debug!(
                            self.ctx,
                            "failed to schedule pod {:?}: {:?}",
                            next_pod_name,
                            err
                        );
                        unscheduled_queue.push_back(next_pod_name);
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
        }

        assert_eq!(
            0,
            self.pod_queue.len(),
            "impossible scenario: queue is not empty after while cycle"
        );
        self.pod_queue = unscheduled_queue;

        let elapsed = cycle_start_time.elapsed();
        let next_cycle_delay =
            f64::max(elapsed.as_secs_f64(), self.config.scheduling_cycle_interval)
                + self.config.sched_to_as_network_delay;

        // TODO: need some better way to stop
        if self.ctx.time() > 10000.0 {
            return;
        }
        self.ctx.emit_self(RunSchedulingCycle {}, next_cycle_delay);
    }
}

pub struct LeastRequestedPriorityScheduler {}
impl AnyScheduler for LeastRequestedPriorityScheduler {
    // TODO: write proc_macros for this
    fn schedule_one<'a>(
        &self,
        pod: &'a Pod,
        nodes: &mut dyn Iterator<Item = &'a Node>,
    ) -> Result<&'a Node, ScheduleError> {
        KubeGenericScheduler::schedule_one(self, pod, nodes)
    }
}
impl KubeGenericScheduler for LeastRequestedPriorityScheduler {
    // Basic least requested priority algorithm. It means that after subtracting pod's
    // requested resources from node's allocatable resources, the node with the highest
    // percentage difference (relatively to capacity) is prioritized for scheduling.
    //
    // Weights for cpu and memory are equal by default.
    fn filter<'a>(&self, pod: &'a Pod, nodes: &mut dyn Iterator<Item = &'a Node>) -> Vec<&'a Node> {
        nodes
            .filter(|&node| {
                pod.spec.resources.requests.cpu <= node.status.allocatable.cpu
                    && pod.spec.resources.requests.ram <= node.status.allocatable.ram
            })
            .collect()
    }

    fn score(&self, pod: &Pod, node: &Node) -> f64 {
        let cpu_score = (node.status.allocatable.cpu - pod.spec.resources.requests.cpu) as f64
            * 100.0
            / node.status.allocatable.cpu as f64;
        let ram_score = (node.status.allocatable.ram - pod.spec.resources.requests.ram) as f64
            * 100.0
            / node.status.allocatable.ram as f64;
        (cpu_score + ram_score) / 2.0
    }
}

impl EventHandler for Scheduler {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            RunSchedulingCycle {} => {
                self.run_scheduling_cycle();
            }
            AddNodeToCacheRequest { node } => {
                self.add_node(node);
            }
            PodScheduleRequest { pod } => {
                let pod_name = pod.metadata.name.clone();
                self.pod_queue.push_back(pod_name);
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
    use std::rc::Rc;

    use dslab_core::Simulation;

    use crate::core::node::Node;
    use crate::core::pod::Pod;
    use crate::core::scheduler::{LeastRequestedPriorityScheduler, ScheduleError, Scheduler};

    use crate::test_util::helpers::default_test_simulation_config;

    fn create_scheduler() -> Scheduler {
        let mut fake_sim = Simulation::new(0);

        Scheduler::new(
            0,
            Box::new(LeastRequestedPriorityScheduler {}),
            fake_sim.create_context("scheduler"),
            Rc::new(default_test_simulation_config()),
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
            ScheduleError::NoSufficientNodes
        );
    }

    #[test]
    fn test_pod_has_requested_zero_resources() {
        let scheduler = create_scheduler();
        let pod = Pod::new("pod_1".to_string(), 0, 0, 5.0);
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
            ScheduleError::NoSufficientNodes
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
        assert_eq!(*scheduler.schedule_one(&pod).ok().unwrap(), node3);
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
        assert_eq!(
            *scheduler.schedule_one(&pod1).ok().unwrap(),
            scheduler.get_node(node_name)
        );
        scheduler.reserve_node_resources(&pod1.metadata.name, node_name);
        assert_eq!(
            *scheduler.schedule_one(&pod2).ok().unwrap(),
            scheduler.get_node(node_name)
        );
        scheduler.reserve_node_resources(&pod2.metadata.name, node_name);
        assert_eq!(
            *scheduler.schedule_one(&pod3).ok().unwrap(),
            scheduler.get_node(node_name)
        );
        scheduler.reserve_node_resources(&pod3.metadata.name, node_name);
        // there is no place left on node for the fourth pod
        assert_eq!(
            scheduler.schedule_one(&pod4).err().unwrap(),
            ScheduleError::NoSufficientNodes
        );
    }
}
