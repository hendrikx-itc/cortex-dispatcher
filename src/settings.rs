use regex::Regex;

extern crate regex;
extern crate serde_regex;


#[derive(Debug, Deserialize)]
pub struct DataTarget {
    #[serde(with = "serde_regex")]
    pub regex: Regex,
    pub directory: String
}

#[derive(Debug, Deserialize)]
pub struct DataSource {
    pub directory: String,
    pub targets: Vec<DataTarget>
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub sources: Vec<DataSource>
}
