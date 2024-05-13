//! Implementation of scheduler component which is responsible for scheduling pods for nodes.

use std::cell::RefCell;
use std::collections::BinaryHeap;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;

use dslab_core::{cast, log_debug, log_trace, Event, EventHandler, SimulationContext};

use crate::core::common::{ObjectsInfo, RuntimeResources, SimComponentId};
use crate::core::events::{
    AddNodeToCache, AssignPodToNodeRequest, FlushUnschedulableQueueLeftover, PodFinishedRunning,
    PodNotScheduled, PodScheduleRequest, RemoveNodeFromCache, RemovePodFromCache,
    RunSchedulingCycle,
};
use crate::core::node::Node;
use crate::core::pod::Pod;
use crate::core::scheduler::interface::{PodSchedulingAlgorithm, ScheduleError};
use crate::core::scheduler::model::{ConstantTimePerNodeModel, PodSchedulingTimeModel};

use crate::metrics::collector::MetricsCollector;

use crate::config::SimulationConfig;

use crate::core::scheduler::queue::{
    QueuedPodInfo, UnschedulablePodKey, DEFAULT_POD_MAX_IN_UNSCHEDULABLE_PODS_DURATION,
    POD_FLUSH_INTERVAL,
};

pub struct Scheduler {
    api_server: SimComponentId,

    /// Cache which is updated based on events from persistent storage
    objects_cache: ObjectsInfo,
    /// Map from node name to pods that were assigned to that node
    assignments: HashMap<String, BTreeSet<String>>,

    scheduler_algorithm: Box<dyn PodSchedulingAlgorithm>,

    pod_scheduling_time_model: Box<dyn PodSchedulingTimeModel>,
    /// Sorted by timestamp of addition a queue contains information about a pod to schedule.
    /// Firstly-added pod is assigned a timestamp of event PodScheduleRequest.
    /// Pods from unschedulable queue are moved to active queue with the timestamp of last adding
    /// to unschedulable queue.
    action_queue: BinaryHeap<QueuedPodInfo>,
    /// Map of pod keys and their queue info which cannot be schedulable at the moment.
    /// Moves to active queue either if DEFAULT_POD_MAX_IN_UNSCHEDULABLE_PODS_DURATION exceeded or
    /// event of interest (PodFinishedRunning, AddNodeToCache) occurred.
    pub unschedulable_pods: BTreeMap<UnschedulablePodKey, QueuedPodInfo>,

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
            assignments: Default::default(),
            scheduler_algorithm,
            pod_scheduling_time_model: Box::new(ConstantTimePerNodeModel::default()),
            action_queue: Default::default(),
            unschedulable_pods: Default::default(),
            ctx,
            config,
            metrics_collector,
        }
    }

    pub fn start(&mut self) {
        self.ctx.emit_self_now(RunSchedulingCycle {});
        self.ctx.emit_self_now(FlushUnschedulableQueueLeftover {});
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
        match self.assignments.get_mut(node_name) {
            Some(assigned_pods) => {
                assigned_pods.insert(pod_name.to_string());
            }
            None => {
                self.assignments.insert(
                    node_name.to_string(),
                    BTreeSet::from([pod_name.to_string()]),
                );
            }
        };

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

    fn release_node_resources(&mut self, pod: &Pod) {
        let node = self
            .objects_cache
            .nodes
            .get_mut(&pod.status.assigned_node)
            .unwrap();

        node.status.allocatable.cpu += pod.spec.resources.requests.cpu;
        node.status.allocatable.ram += pod.spec.resources.requests.ram;
    }

    fn schedule_one(&self, pod: &Pod) -> Result<String, ScheduleError> {
        log_trace!(
            self.ctx,
            "Considering {} nodes for scheduling",
            self.objects_cache.nodes.len()
        );
        self.scheduler_algorithm
            .schedule_one(pod, &self.objects_cache.nodes)
    }

    fn move_pods_to_active_queue(&mut self, unscheduled_pods: Vec<UnschedulablePodKey>) {
        for key in unscheduled_pods.into_iter() {
            // Check whether pod was removed from RemovePodFromCache event
            if !self.objects_cache.pods.contains_key(&key.pod_name as &str) {
                continue;
            }

            let mut queue_pod_info = self.unschedulable_pods.remove(&key).unwrap();
            queue_pod_info.attempts += 1;
            self.action_queue.push(queue_pod_info);
        }
    }

    /// Flushes pods to the active queue which stays in unschedulable queue for too long.
    fn flush_unschedulable_pods_leftover(&mut self, event_time: f64) {
        let mut pods_to_move: Vec<UnschedulablePodKey> = vec![];
        pods_to_move.reserve(self.unschedulable_pods.len());

        for (key, queued_pod_info) in self.unschedulable_pods.iter() {
            let pod_stay_duration = event_time - queued_pod_info.timestamp;
            if pod_stay_duration > DEFAULT_POD_MAX_IN_UNSCHEDULABLE_PODS_DURATION {
                pods_to_move.push(key.clone());
            }
        }

        self.move_pods_to_active_queue(pods_to_move);

        self.ctx
            .emit_self(FlushUnschedulableQueueLeftover {}, POD_FLUSH_INTERVAL);
    }

    /// Move pods from unschedulable to active queue which satisfy check.
    /// On events of interest such as some resources on nodes are freed or new nodes are added.
    /// Assuming `check` returns true if should move current pod.
    /// Inside lambda's captured state (pod/node) resources decrease on amount of resources of pods
    /// which are moved.
    fn move_to_active_queue_if_sufficient_resources(
        &mut self,
        mut check: impl FnMut(&RuntimeResources) -> bool,
    ) {
        let mut pods_to_move: Vec<UnschedulablePodKey> = vec![];
        pods_to_move.reserve(self.unschedulable_pods.len());

        for (key, queued_pod_info) in self.unschedulable_pods.iter() {
            if check(
                &self
                    .objects_cache
                    .pods
                    .get(&*queued_pod_info.pod_name)
                    .unwrap()
                    .spec
                    .resources
                    .requests,
            ) {
                pods_to_move.push(key.clone());
            }
        }

        self.move_pods_to_active_queue(pods_to_move);
    }

    fn run_scheduling_cycle(&mut self, scheduling_cycle_event_time: f64) {
        let mut cycle_sim_duration = 0.0;

        log_debug!(
            self.ctx,
            "run scheduling cycle, active queue len={:?}, unschedulable queue len={:?}",
            self.action_queue.len(),
            self.unschedulable_pods.len()
        );

        while let Some(mut next_pod) = self.action_queue.pop() {
            // Check whether pod was removed from RemovePodFromCache event
            if !self
                .objects_cache
                .pods
                .contains_key(&next_pod.pod_name as &str)
            {
                continue;
            }

            let pod_queue_time = scheduling_cycle_event_time - next_pod.initial_attempt_timestamp
                + cycle_sim_duration;

            let pod = self.objects_cache.pods.get(&*next_pod.pod_name).unwrap();
            let pod_schedule_time = self
                .pod_scheduling_time_model
                .simulate_time(pod, &self.objects_cache.nodes);
            cycle_sim_duration += pod_schedule_time;

            let assigned_node = match self.schedule_one(pod) {
                Ok(assigned_node) => assigned_node,
                Err(err) => {
                    log_trace!(
                        self.ctx,
                        "failed to schedule pod {:?}: {:?}",
                        next_pod.pod_name,
                        err
                    );
                    next_pod.timestamp = scheduling_cycle_event_time + cycle_sim_duration;
                    self.unschedulable_pods.insert(
                        UnschedulablePodKey {
                            pod_name: next_pod.pod_name.clone(),
                            insert_timestamp: next_pod.timestamp,
                        },
                        next_pod,
                    );
                    self.ctx.emit(
                        PodNotScheduled {
                            not_scheduled_time: scheduling_cycle_event_time + cycle_sim_duration,
                            pod_name: pod.metadata.name.clone(),
                        },
                        self.api_server,
                        self.config.sched_to_as_network_delay,
                    );
                    continue;
                }
            };

            self.reserve_node_resources(&next_pod.pod_name, &assigned_node);
            self.assign_node_to_pod(&next_pod.pod_name, &assigned_node);

            self.ctx.emit(
                AssignPodToNodeRequest {
                    assign_time: scheduling_cycle_event_time + cycle_sim_duration,
                    pod_name: next_pod.pod_name.to_string(),
                    node_name: assigned_node,
                },
                self.api_server,
                cycle_sim_duration + self.config.sched_to_as_network_delay,
            );

            self.metrics_collector
                .borrow_mut()
                .increment_pod_scheduling_algorithm_latency(pod_schedule_time);
            self.metrics_collector
                .borrow_mut()
                .increment_pod_queue_time(pod_queue_time);
        }

        let next_cycle_delay = f64::max(cycle_sim_duration, self.config.scheduling_cycle_interval);
        self.ctx.emit_self(RunSchedulingCycle {}, next_cycle_delay);
    }

    fn reschedule_pod(&mut self, pod_name: String, event_time: f64) {
        self.objects_cache
            .pods
            .get_mut(&pod_name)
            .unwrap()
            .status
            .assigned_node = Default::default();

        self.action_queue.push(QueuedPodInfo {
            timestamp: event_time,
            attempts: 1,
            initial_attempt_timestamp: event_time,
            pod_name: Rc::new(pod_name),
        });
    }

    fn reschedule_unfinished_pods(&mut self, node_name: &str, event_time: f64) {
        if let Some(unfinished_pod_names) = self.assignments.remove(node_name) {
            log_debug!(
                self.ctx,
                "Rescheduling unfinished pods which were assigned previously on node {:?}: {:?}",
                node_name,
                unfinished_pod_names
            );
            for pod_name in unfinished_pod_names.into_iter() {
                self.reschedule_pod(pod_name, event_time);
            }
        }
    }

    fn move_to_active_due_to_pod_freed_resources(&mut self, freed: RuntimeResources) {
        let mut freed_resources = freed;
        let check = |requested_resources: &RuntimeResources| {
            if requested_resources.cpu <= freed_resources.cpu
                && requested_resources.ram <= freed_resources.ram
            {
                freed_resources.cpu -= requested_resources.cpu;
                freed_resources.ram -= requested_resources.ram;
                return true;
            }
            return false;
        };
        self.move_to_active_queue_if_sufficient_resources(check);
    }
}

impl EventHandler for Scheduler {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            RunSchedulingCycle {} => {
                self.run_scheduling_cycle(event.time);
            }
            FlushUnschedulableQueueLeftover {} => {
                self.flush_unschedulable_pods_leftover(event.time);
            }
            AddNodeToCache { node } => {
                let mut allocatable = node.status.allocatable.clone();
                self.add_node(node);

                let check = |requested_resources: &RuntimeResources| {
                    if requested_resources.cpu <= allocatable.cpu
                        && requested_resources.ram <= allocatable.ram
                    {
                        allocatable.cpu -= requested_resources.cpu;
                        allocatable.ram -= requested_resources.ram;
                        return false;
                    }
                    return true;
                };
                self.move_to_active_queue_if_sufficient_resources(check);
            }
            PodScheduleRequest { pod } => {
                let pod_name = pod.metadata.name.clone();
                self.add_pod(pod);

                self.action_queue.push(QueuedPodInfo {
                    timestamp: event.time,
                    attempts: 1,
                    initial_attempt_timestamp: event.time,
                    pod_name: Rc::new(pod_name),
                });
            }
            PodFinishedRunning {
                pod_name,
                node_name,
                ..
            } => {
                let pod = self.objects_cache.pods.remove(&pod_name).unwrap();

                self.assignments
                    .get_mut(&node_name)
                    .unwrap()
                    .remove(&pod_name);
                self.release_node_resources(&pod);

                self.move_to_active_due_to_pod_freed_resources(pod.spec.resources.requests.clone());
            }
            RemoveNodeFromCache { node_name } => {
                self.objects_cache.nodes.remove(&node_name).unwrap();
                self.reschedule_unfinished_pods(&node_name, event.time);
            }
            RemovePodFromCache { pod_name } => {
                // Remove request might come after finish request. So we check whether pod is still
                // in objects cache. If it's finished earlier than it's removed from cache.
                if let Some(pod) = self.objects_cache.pods.remove(&pod_name) {
                    // Pod is still not finished - should clean up info about it.
                    let assigned_node_name = &pod.status.assigned_node;
                    // Releasing resources is optional as we could receive remove node request earlier
                    // then remove pod request. In remove request handling we remove node from cache.
                    // So if assigned node name is not empty then this node is still alive, need to
                    // cleanup information.
                    if !assigned_node_name.is_empty() {
                        self.release_node_resources(&pod);
                        self.assignments
                            .get_mut(assigned_node_name)
                            .unwrap()
                            .remove(&pod_name);

                        self.move_to_active_due_to_pod_freed_resources(
                            pod.spec.resources.requests.clone(),
                        );
                    }
                    // Otherwise, pod is in one of scheduling queues. So when we process popping
                    // from queue - just skip it with the help of checking existence in objects cache.
                }
                // Otherwise, already finished - do nothing.
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
            Rc::new(default_test_simulation_config(None)),
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
