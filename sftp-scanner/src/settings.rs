#[derive(Debug, Deserialize, Clone)]
pub struct CommandQueue {
    pub address: String,
    pub queue_name: String
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub command_queue: CommandQueue,
}
