use crate::{Error, Result};

pub struct Parser {
    // TODO Some statistics here
}

impl Parser {
    pub fn new() -> Parser {
        Self {}
    }

    pub fn parse<'a>(&self, lines: &'a str) -> Result<Vec<Line<'a>>> {
        let mut ret: Vec<Line> = Vec::new();
        let mut pos = 0_usize;
        while let Some((line, offset)) = next_line(lines, pos)? {
            ret.push(line);
            pos += offset;
        }
        Ok(ret)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Line<'a> {
    pub measurement: &'a str,
    pub tags: Vec<(&'a str, &'a str)>,
    pub fields: Vec<(&'a str, &'a str)>,
    pub timestamp: Option<&'a str>,
}

fn next_line<'a>(buf: &'a str, position: usize) -> Result<Option<(Line<'a>, usize)>> {
    if position > buf.len() {
        return Ok(None);
    }

    let mut pos = position;
    let measurement = if let Some(m) = next_measurement(&buf[pos..]) {
        pos += m.1;
        m.0
    } else {
        return Ok(None);
    };
    check_pos_valid(&buf, pos)?;

    let tags = if let Some(t) = next_tag_set(&buf[pos..]) {
        pos += t.1;
        t.0
    } else {
        return Err(Error::Parse {
            pos: pos,
            content: String::from(buf),
        });
    };
    check_pos_valid(&buf, pos)?;

    let fields = if let Some(f) = next_field_set(&buf[pos..]) {
        pos += f.1;
        f.0
    } else {
        return Err(Error::Parse {
            pos: pos,
            content: String::from(buf),
        });
    };

    let timestamp = if pos < buf.len() {
        let timestamp = next_timestamp(&buf[pos..]);
        pos += timestamp.unwrap().1;
        timestamp.map(|t| t.0)
    } else {
        None
    };

    Ok(Some((
        Line {
            measurement,
            tags,
            fields,
            timestamp,
        },
        pos + 1,
    )))
}

fn check_pos_valid(buf: &str, pos: usize) -> Result<()> {
    if pos < buf.len() {
        return Ok(());
    }
    return Err(Error::Parse {
        pos: pos,
        content: String::from(buf),
    });
}

fn next_measurement(buf: &str) -> Option<(&str, usize)> {
    let mut escaped = false;
    let mut tok_measurement = false;
    let (mut tok_begin, mut tok_end) = (0, buf.len());
    for (i, c) in buf.chars().enumerate() {
        // Measurement begin character
        if c == '\\' {
            escaped = true;
            if !tok_measurement {
                tok_measurement = true;
                tok_begin = i;
            }
            continue;
        }
        if tok_measurement {
            // Measurement end character
            if c == ',' {
                tok_end = i;
                break;
            }
        } else {
            // Measurement begin character
            if c.is_alphanumeric() {
                tok_measurement = true;
                tok_begin = i;
            }
        }
        if escaped {
            escaped = false;
        }
    }
    if tok_measurement {
        Some((&buf[tok_begin..tok_end], tok_end + 1))
    } else {
        None
    }
}

fn is_tagset_character(c: char) -> bool {
    c.is_alphanumeric() || c == '\\'
}

fn next_tag_set(buf: &str) -> Option<(Vec<(&str, &str)>, usize)> {
    let mut escaped = false;
    let mut exists_tagset = false;
    let mut tok_offsets = [0_usize; 3];
    let mut tok_end = 0_usize;

    let mut tag_set: Vec<(&str, &str)> = Vec::new();
    for (i, c) in buf.chars().enumerate() {
        // TagSet begin character
        if !escaped && c == '\\' {
            escaped = true;
            if !exists_tagset {
                exists_tagset = true;
                tok_offsets[0] = i;
            }
            continue;
        }
        if exists_tagset {
            if !escaped && c == '=' {
                tok_offsets[1] = i;
                if buf.len() <= i + 1 {
                    return None;
                }
                tok_offsets[2] = i + 1;
            }
            if !escaped && c == ',' {
                tag_set.push((
                    &buf[tok_offsets[0]..tok_offsets[1]],
                    &buf[tok_offsets[2]..i],
                ));
                if buf.len() <= i + 1 {
                    return None;
                }
                tok_offsets[0] = i + 1;
            }

            // TagSet end character
            if !escaped && c == ' ' {
                tok_end = i;
                break;
            }
        } else {
            // TagSet begin character
            if is_tagset_character(c) {
                exists_tagset = true;
                tok_offsets[0] = i;
            }
        }
        if escaped {
            escaped = false;
        }
    }
    if exists_tagset {
        if tok_end == 0 {
            tok_end = buf.len()
        }
        tag_set.push((
            &buf[tok_offsets[0]..tok_offsets[1]],
            &buf[tok_offsets[2]..tok_end],
        ));
        Some((tag_set, tok_end + 1))
    } else {
        None
    }
}

fn next_field_set(buf: &str) -> Option<(Vec<(&str, &str)>, usize)> {
    let mut escaped = false;
    let mut quoted = false;
    let mut exists_field_set = false;
    let mut tok_offsets = [0_usize; 3];
    let mut tok_end = 0_usize;

    let mut field_set: Vec<(&str, &str)> = Vec::new();
    for (i, c) in buf.chars().enumerate() {
        // TagSet begin character
        if c == '\\' {
            escaped = true;
            if !exists_field_set {
                exists_field_set = true;
                tok_offsets[0] = i;
            }
            continue;
        }
        if exists_field_set {
            if !escaped && c == '"' {
                quoted = !quoted;
                continue;
            }

            if !escaped && c == '=' {
                tok_offsets[1] = i;
                if buf.len() <= i + 1 {
                    return None;
                }
                tok_offsets[2] = i + 1;
            }
            if !escaped && c == ',' {
                field_set.push((
                    &buf[tok_offsets[0]..tok_offsets[1]],
                    &buf[tok_offsets[2]..i],
                ));
                if buf.len() <= i + 1 {
                    return None;
                }
                tok_offsets[0] = i + 1;
            }

            // FieldSet end character
            if !quoted && c == ' ' || c == '\n' {
                tok_end = i;
                break;
            }
        } else {
            // FieldSet begin character
            if c.is_alphanumeric() {
                exists_field_set = true;
                tok_offsets[0] = i;
            }
        }
        if escaped {
            escaped = false;
        }
    }
    if exists_field_set {
        if tok_end == 0 {
            tok_end = buf.len()
        }
        field_set.push((
            &buf[tok_offsets[0]..tok_offsets[1]],
            &buf[tok_offsets[2]..tok_end],
        ));
        Some((field_set, tok_end + 1))
    } else {
        None
    }
}

fn next_timestamp(buf: &str) -> Option<(&str, usize)> {
    let mut tok_time = false;
    let (mut tok_begin, mut tok_end) = (0, buf.len());
    for (i, c) in buf.chars().enumerate() {
        if tok_time {
            if c == ' ' || c == '\n' || (!c.is_numeric() && c != '-') {
                tok_end = i;
                break;
            }
        } else {
            if c.is_numeric() {
                tok_begin = i;
                tok_time = true;
            }
        }
    }
    if tok_time {
        Some((&buf[tok_begin..tok_end], tok_end + 1))
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use crate::parser::{
        next_field_set, next_measurement, next_tag_set, next_timestamp, Line, Parser,
    };

    #[test]
    fn test_parse_functions() {
        //! measurement: ma
        //! | ta  | tb | fa       | fb | ts |
        //! | --  | -- | ------   | -- | -- |
        //! | 2\\ | 1  | "112\"3" | 2  | 1  |
        //!
        //! measurement: mb
        //! | tb | tc  | fa  | fc  | ts |
        //! | -- | --- | --- | --- | -- |
        //! | 2  | abc | 1.3 | 0.9 |    |

        let lines = "ma,ta=2\\\\,tb=1 fa=\"112\\\"3\",fb=2 1  \n mb,tb=2,tc=abc fa=1.3,fc=0.9";
        println!(
            "Length of the line protocol string in test case: {}\n======\n{}\n======",
            lines.len(),
            lines
        );

        let mut pos = 0;
        let measurement = next_measurement(&lines[pos..]).unwrap();
        assert_eq!(measurement, ("ma", 3));
        pos += measurement.1;

        if pos < lines.len() {
            let tagset = next_tag_set(&lines[pos..]).unwrap();
            assert_eq!(tagset, (vec![("ta", "2\\\\"), ("tb", "1")], 12));
            pos += tagset.1;
        }

        if pos < lines.len() {
            let fieldset = next_field_set(&lines[pos..]).unwrap();
            assert_eq!(fieldset, (vec![("fa", "\"112\\\"3\""), ("fb", "2")], 17));
            pos += fieldset.1;
        }

        if pos < lines.len() {
            let timestamp = next_timestamp(&lines[pos..]);
            assert_eq!(timestamp, Some(("1", 2)));
            pos += timestamp.unwrap().1;
        }

        println!("==========");

        if pos < lines.len() {
            let measurement = next_measurement(&lines[pos..]).unwrap();
            assert_eq!(measurement, ("mb", 6));
            pos += measurement.1;
        }

        if pos < lines.len() {
            let tagset = next_tag_set(&lines[pos..]).unwrap();
            assert_eq!(tagset, (vec![("tb", "2"), ("tc", "abc")], 12));
            pos += tagset.1;
        }

        if pos < lines.len() {
            let fieldset = next_field_set(&lines[pos..]).unwrap();
            assert_eq!(fieldset, (vec![("fa", "1.3"), ("fc", "0.9")], 14));
            pos += fieldset.1;
        }

        if pos < lines.len() {
            let timestamp = next_timestamp(&lines[pos..]);
            assert_eq!(timestamp, None);
        }
    }

    #[test]
    fn test_line_parser() {
        //! measurement: ma
        //! | ta  | tb | fa       | fb | ts |
        //! | --  | -- | ------   | -- | -- |
        //! | 2\\ | 1  | "112\"3" | 2  | 1  |
        //!
        //! measurement: mb
        //! | tb | tc  | fa  | fc  | ts |
        //! | -- | --- | --- | --- | -- |
        //! | 2  | abc | 1.3 | 0.9 |    |

        let lines = "ma,ta=2\\\\,tb=1 fa=\"112\\\"3\",fb=2 1  \n mb,tb=2,tc=abc fa=1.3,fc=0.9";
        println!(
            "Length of the line protocol string in test case: {}\n======\n{}\n======",
            lines.len(),
            lines
        );

        let parser = Parser::new();
        let data = parser.parse(lines).unwrap();
        assert_eq!(data.len(), 2);

        let data_1 = data.get(0).unwrap();
        assert_eq!(
            *data_1,
            Line {
                measurement: "ma",
                tags: vec![("ta", "2\\\\"), ("tb", "1")],
                fields: vec![("fa", "\"112\\\"3\""), ("fb", "2")],
                timestamp: Some("1")
            }
        );

        let data_2 = data.get(1).unwrap();
        assert_eq!(
            *data_2,
            Line {
                measurement: "mb",
                tags: vec![("tb", "2"), ("tc", "abc")],
                fields: vec![("fa", "1.3"), ("fc", "0.9")],
                timestamp: None
            }
        );
    }
}
