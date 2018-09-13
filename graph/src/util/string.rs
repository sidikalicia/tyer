use std::fmt;
use std::ops::Deref;

/// A vector of strings that implements Display.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Strings(pub Vec<String>);

impl fmt::Display for Strings {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let s = (&self.0).join(", ");
        write!(f, "{}", s)
    }
}

impl Deref for Strings {
    type Target = Vec<String>;

    fn deref(&self) -> &Vec<String> {
        &self.0
    }
}
