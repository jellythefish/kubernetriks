//! Definitions for helper structs used in scheduler queues.

use std::{cmp::Ordering, rc::Rc};

/// Default value (secs) for the maximum time a pod can stay in unschedulablePods. If a pod stays in
/// unschedulablePods for longer than this value, no matter resources update events happened or not,
/// the pod will be moved from unschedulablePods to backoffQ or activeQ.
pub const DEFAULT_POD_MAX_IN_UNSCHEDULABLE_PODS_DURATION: f64 = 5.0 * 60.0;

/// Value (secs) for the running cycle to flush pods that stay for too long.
pub const POD_FLUSH_INTERVAL: f64 = 30.0;

#[derive(Clone)]
pub struct QueuedPodInfo {
    /// The time pod added to the scheduling queue.
    pub timestamp: f64,
    /// Number of schedule attempts before successfully scheduled.
    /// It's used to record the # attempts metric.
    pub attempts: usize,
    /// The time when the pod is added to the queue for the first time. The pod may be added
    /// back to the queue multiple times before it's successfully scheduled.
    /// It shouldn't be updated once initialized. It's used to record the e2e scheduling
    /// latency for a pod.
    pub initial_attempt_timestamp: f64,
    /// Reference to a name of a pod which object is stored in scheduler's objects_cache
    pub pod_name: Rc<String>,
}

impl Ord for QueuedPodInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.timestamp.total_cmp(&self.timestamp)
    }
}

impl PartialOrd for QueuedPodInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for QueuedPodInfo {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp && self.pod_name == other.pod_name
    }
}

impl Eq for QueuedPodInfo {}

#[derive(Clone, Debug)]
pub struct UnschedulablePodKey {
    pub pod_name: Rc<String>,
    pub insert_timestamp: f64,
}

impl Ord for UnschedulablePodKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.insert_timestamp
            .total_cmp(&other.insert_timestamp)
            .then(self.pod_name.cmp(&other.pod_name))
    }
}

impl PartialOrd for UnschedulablePodKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for UnschedulablePodKey {
    fn eq(&self, other: &Self) -> bool {
        self.pod_name == other.pod_name && self.insert_timestamp == other.insert_timestamp
    }
}

impl Eq for UnschedulablePodKey {}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BinaryHeap},
        rc::Rc,
    };

    use super::{QueuedPodInfo, UnschedulablePodKey};

    #[test]
    fn test_queue_pod_info_order() {
        let mut queue = BinaryHeap::<QueuedPodInfo>::new();
        let mut queue_pod_info = QueuedPodInfo {
            timestamp: 1.0,
            attempts: 1,
            initial_attempt_timestamp: 1.0,
            pod_name: Rc::new("some_pod".to_string()),
        };

        queue.push(queue_pod_info.clone());
        queue_pod_info.timestamp = 5.0;
        queue.push(queue_pod_info.clone());
        queue_pod_info.timestamp = 4.0;
        queue.push(queue_pod_info.clone());
        queue_pod_info.timestamp = 0.5;
        queue.push(queue_pod_info.clone());
        queue_pod_info.timestamp = 4.0;
        queue.push(queue_pod_info.clone());

        assert_eq!(0.5, queue.pop().unwrap().timestamp);
        assert_eq!(1.0, queue.pop().unwrap().timestamp);
        assert_eq!(4.0, queue.pop().unwrap().timestamp);
        assert_eq!(4.0, queue.pop().unwrap().timestamp);
        assert_eq!(5.0, queue.pop().unwrap().timestamp);
        assert!(queue.pop().is_none());
    }

    fn insert_into_queue(
        queue: &mut BTreeMap<UnschedulablePodKey, QueuedPodInfo>,
        info: &QueuedPodInfo,
    ) {
        queue.insert(
            UnschedulablePodKey {
                pod_name: info.pod_name.clone(),
                insert_timestamp: info.timestamp,
            },
            info.clone(),
        );
    }

    #[test]
    fn test_unschedulable_queue_order() {
        let mut queue = BTreeMap::<UnschedulablePodKey, QueuedPodInfo>::new();

        let mut queue_pod_info = QueuedPodInfo {
            timestamp: 1.0,
            attempts: 1,
            initial_attempt_timestamp: 1.0,
            pod_name: Rc::new("some_pod".to_string()),
        };
        insert_into_queue(&mut queue, &queue_pod_info);

        queue_pod_info.timestamp = 10.0;
        queue_pod_info.pod_name = Rc::new("some_pod_2".to_string());
        insert_into_queue(&mut queue, &queue_pod_info);
        queue_pod_info.timestamp = 7.0;
        queue_pod_info.pod_name = Rc::new("some_pod_5".to_string());
        insert_into_queue(&mut queue, &queue_pod_info);
        queue_pod_info.timestamp = 5.0;
        queue_pod_info.pod_name = Rc::new("some_pod_3".to_string());
        insert_into_queue(&mut queue, &queue_pod_info);
        queue_pod_info.timestamp = 7.0;
        queue_pod_info.pod_name = Rc::new("some_pod_4".to_string());
        insert_into_queue(&mut queue, &queue_pod_info);

        let entries: Vec<(UnschedulablePodKey, QueuedPodInfo)> = queue.into_iter().collect();

        assert_eq!("some_pod", *entries[0].0.pod_name);
        assert_eq!("some_pod_3", *entries[1].0.pod_name);
        assert_eq!("some_pod_4", *entries[2].0.pod_name);
        assert_eq!("some_pod_5", *entries[3].0.pod_name);
        assert_eq!("some_pod_2", *entries[4].0.pod_name);
        assert_eq!(1.0, entries[0].0.insert_timestamp);
        assert_eq!(5.0, entries[1].0.insert_timestamp);
        assert_eq!(7.0, entries[2].0.insert_timestamp);
        assert_eq!(7.0, entries[3].0.insert_timestamp);
        assert_eq!(10.0, entries[4].0.insert_timestamp);
    }
}
