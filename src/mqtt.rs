pub fn validate_topic_name(topic: &str) -> bool {
    let mut last_part_length = 0;
    for c in topic.chars() {
        if c == '+' || c == '#' {
            return false;
        }

        if c == '/' {
            if last_part_length == 0 {
                return false;
            } else {
                last_part_length = 0;
            }
        } else {
            last_part_length += 1;
        }
    }

    if last_part_length == 0 {
        return false;
    }

    true
}
