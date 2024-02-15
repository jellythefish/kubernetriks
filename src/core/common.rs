use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
pub struct Resources {
    pub cpu: u32, // in millicores
    pub ram: u64, // in bytes
}

impl Default for Resources {
    fn default() -> Self {
        Self {
            cpu: 0,
            ram: 0,
        }
    }
}
