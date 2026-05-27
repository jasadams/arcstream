const ADJECTIVES: &[&str] = &[
    "amber", "arctic", "autumn", "blazing", "bold", "brave", "bright",
    "bronze", "calm", "cedar", "clever", "coastal", "cobalt", "coral",
    "cosmic", "crisp", "crystal", "daring", "dawn", "deep", "deft",
    "desert", "distant", "dusk", "dusty", "eager", "early", "elder",
    "ember", "epic", "fading", "faint", "feral", "fierce", "fleet",
    "flint", "foggy", "forest", "frost", "gentle", "gilded", "glacial",
    "golden", "granite", "hazy", "hidden", "hollow", "hushed", "icy",
    "idle", "iron", "ivory", "jade", "keen", "last", "late", "lean",
    "lofty", "lone", "lost", "lunar", "maple", "mellow", "misty",
    "mossy", "noble", "north", "nova", "oaken", "obsidian", "olive",
    "onyx", "pale", "patient", "peak", "pine", "plain", "polar",
    "proud", "quiet", "rapid", "rare", "regal", "rocky", "roving",
    "ruby", "rustic", "sage", "sandy", "scarlet", "serene", "shadow",
    "sharp", "silent", "silver", "sleek", "solar", "stark", "steady",
    "steel", "still", "stone", "storm", "stout", "sunlit", "swift",
    "tawny", "tidal", "timber", "topaz", "twin", "upper", "vast",
    "velvet", "verdant", "vivid", "wandering", "warm", "wild", "windy",
    "winter", "wiry", "worn", "young", "zenith",
];

const ANIMALS: &[&str] = &[
    "badger", "bear", "bison", "bobcat", "buck", "crane", "crow",
    "condor", "coyote", "deer", "dove", "eagle", "egret", "elk",
    "falcon", "ferret", "finch", "fox", "gecko", "goat", "goose",
    "grouse", "gull", "hare", "hawk", "heron", "horse", "hound",
    "ibex", "ibis", "jackal", "jay", "kestrel", "kite", "lark",
    "lemur", "leopard", "lion", "llama", "lynx", "marten", "merlin",
    "mink", "moose", "moth", "newt", "ocelot", "orca", "osprey",
    "otter", "owl", "ox", "panther", "parrot", "pelican", "pike",
    "puma", "quail", "raven", "robin", "salmon", "seal", "shrike",
    "snipe", "sparrow", "spider", "stag", "stork", "swift", "tern",
    "thrush", "tiger", "toad", "trout", "viper", "vole", "vulture",
    "walrus", "weasel", "whale", "wolf", "wren", "yak", "zebra",
];

fn hash_pair(s: &str) -> (usize, usize) {
    let mut h1: u32 = 0x9e37_79b9;
    let mut h2: u32 = 0x517c_c1b7;
    for b in s.bytes() {
        h1 = h1.wrapping_mul(31).wrapping_add(b as u32);
        h2 = h2.wrapping_mul(37).wrapping_add(b as u32);
    }
    (h1 as usize, h2 as usize)
}

pub fn petname(id: &str) -> String {
    let (h1, h2) = hash_pair(id);
    let adj = ADJECTIVES[h1 % ADJECTIVES.len()];
    let animal = ANIMALS[h2 % ANIMALS.len()];
    format!("{}-{}", adj, animal)
}
