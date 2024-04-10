//! Implementation of kube-scheduler component which is responsible for scheduling pods for nodes.

use std::borrow::Borrow;
use std::collections::HashMap;

use crate::core::node::Node;
use crate::core::pod::Pod;

use crate::core::scheduler::interface::{PodSchedulingAlgorithm, ScheduleError};
use crate::core::scheduler::plugin::{PluginType, PLUGIN_REGISTRY};

pub struct KubeScheduler {
    // Profiles are scheduling profiles that kube-scheduler supports. Pods can
    // choose to be scheduled under a particular profile by setting its associated
    // scheduler name in labels. Pods that don't specify any scheduler name are scheduled
    // with the "default-scheduler" profile, if present here.
    pub config: KubeSchedulerConfig,
}

pub struct KubeSchedulerConfig {
    // Map from scheduler name to scheduler profile
    pub profiles: HashMap<String, KubeSchedulerProfile>,
}

pub struct KubeSchedulerProfile {
    pub scheduler_name: String,
    pub plugins: Plugins,
}

pub struct Plugins {
    // Each extension point is a list of enabled plugins which are registered globally in plugin registry.
    pub filter: Vec<Plugin>,
    pub score: Vec<Plugin>,
}

// Plugin specifies a plugin name and its weight when applicable.
pub struct Plugin {
    pub name: String,
    // Weight is used only for Score plugins.
    pub weight: Option<f64>,
}

pub fn default_kube_scheduler_config() -> KubeSchedulerConfig {
    let scheduler_name = "default_scheduler".to_string();
    let default_profile = KubeSchedulerProfile {
        scheduler_name: scheduler_name.clone(),
        plugins: Plugins {
            filter: vec![Plugin {
                name: "Fit".to_string(),
                weight: None,
            }],
            score: vec![Plugin {
                name: "LeastAllocatedResources".to_string(),
                weight: Some(1.0),
            }],
        },
    };
    KubeSchedulerConfig {
        profiles: HashMap::from([(scheduler_name, default_profile)]),
    }
}

impl KubeScheduler {
    pub fn new(config: KubeSchedulerConfig) -> Self {
        Self { config }
    }

    fn schedule_one(
        &self,
        pod: &Pod,
        nodes: Vec<&Node>,
    ) -> Result<String, ScheduleError> {
        let requested_resources = &pod.spec.resources.requests;
        if requested_resources.cpu == 0 && requested_resources.ram == 0 {
            return Err(ScheduleError::RequestedResourcesAreZeros);
        }

        let default_scheduler_name = "default_scheduler".to_string();
        let pod_scheduler = pod
            .metadata
            .labels
            .get("scheduler_name")
            .unwrap_or(&default_scheduler_name);

        let mut filtered_nodes = nodes;

        for filter in self
            .config
            .profiles
            .get(pod_scheduler)
            .unwrap()
            .plugins
            .filter
            .iter()
        {
            let plugin = PLUGIN_REGISTRY.get(&filter.name as &str).unwrap();
            match plugin {
                PluginType::FilterPlugin(filter_plugin) => {
                    filtered_nodes = filter_plugin.filter(pod, filtered_nodes);
                }
                _ => panic!("{:?} plugin is not of FilterPlugin enum type", filter.name),
            }
        }

        if filtered_nodes.len() == 0 {
            return Err(ScheduleError::NoSufficientNodes);
        }

        let mut node_scores: HashMap<&str, (&Node, f64)> = Default::default();

        for scorer in self
            .config
            .profiles
            .get(pod_scheduler)
            .unwrap()
            .plugins
            .score
            .iter()
        {
            let plugin = PLUGIN_REGISTRY.get(&scorer.name as &str).unwrap();
            match plugin {
                PluginType::ScorePlugin(score_plugin) => {
                    for node in filtered_nodes.iter() {
                        let node_name = node.metadata.name.borrow();
                        if !node_scores.contains_key(node_name) {
                            node_scores.insert(node_name, (node, 0.0));
                        }
                        let node_score = score_plugin.score(pod, node) * scorer.weight.unwrap();
                        (*node_scores.get_mut(node_name).unwrap()).1 += node_score;
                    }
                }
                _ => panic!("{:?} plugin is not of ScorePlugin enum type", scorer.name),
            }
        }

        let mut assigned_node = &filtered_nodes[0].metadata.name;
        let mut max_score = node_scores
            .get(&assigned_node as &str)
            .unwrap()
            .1;

        for (_, (node, score)) in node_scores {
            if score >= max_score {
                assigned_node = &node.metadata.name;
                max_score = score;
            }
        }

        Ok(assigned_node.to_string())
    }
}

impl PodSchedulingAlgorithm for KubeScheduler {
    // TODO: write proc_macros for this
    fn schedule_one(
        &self,
        pod: &Pod,
        nodes: Vec<&Node>,
    ) -> Result<String, ScheduleError> {
        KubeScheduler::schedule_one(self, pod, nodes)
    }
}
