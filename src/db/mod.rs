// Philosophically, I would like this module to be decoupled from Rocket. But
// Rocket does some magic to kinda-sorta merge diesel and diesel-async, so I'm 
// not sure that will be possible.

pub fn latest_ingests() -> Vec<()> {
    vec![]
}