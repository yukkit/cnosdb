use flatbuffers::FlatBufferBuilder;
use line_protocol::Line;
use protos::models as fb_models;
use protos::models::{FieldBuilder, Point, PointArgs, Points, PointsArgs, TagBuilder};

/// move from cnosdb/main/src/http/http_service.rs
pub fn parse_lines_to_points<'a>(db: &'a str, lines: &'a mut [Line]) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::new();
    let mut point_offsets = Vec::with_capacity(lines.len());
    for line in lines.iter_mut() {
        let mut tags = Vec::with_capacity(line.tags.len());
        for (k, v) in line.tags.iter() {
            let fbk = fbb.create_vector(k.as_bytes());
            let fbv = fbb.create_vector(v.as_bytes());
            let mut tag_builder = TagBuilder::new(&mut fbb);
            tag_builder.add_key(fbk);
            tag_builder.add_value(fbv);
            tags.push(tag_builder.finish());
        }
        let mut fields = Vec::with_capacity(line.fields.len());
        for (k, v) in line.fields.iter() {
            let fbk = fbb.create_vector(k.as_bytes());
            let (fbv_type, fbv) = match v {
                line_protocol::FieldValue::U64(field_val) => (
                    fb_models::FieldType::Unsigned,
                    fbb.create_vector(&field_val.to_be_bytes()),
                ),
                line_protocol::FieldValue::I64(field_val) => (
                    fb_models::FieldType::Integer,
                    fbb.create_vector(&field_val.to_be_bytes()),
                ),
                line_protocol::FieldValue::Str(field_val) => {
                    (fb_models::FieldType::String, fbb.create_vector(field_val))
                }
                line_protocol::FieldValue::F64(field_val) => (
                    fb_models::FieldType::Float,
                    fbb.create_vector(&field_val.to_be_bytes()),
                ),
                line_protocol::FieldValue::Bool(field_val) => (
                    fb_models::FieldType::Boolean,
                    if *field_val {
                        fbb.create_vector(&[1_u8][..])
                    } else {
                        fbb.create_vector(&[0_u8][..])
                    },
                ),
            };
            let mut field_builder = FieldBuilder::new(&mut fbb);
            field_builder.add_name(fbk);
            field_builder.add_type_(fbv_type);
            field_builder.add_value(fbv);
            fields.push(field_builder.finish());
        }
        let point_args = PointArgs {
            db: Some(fbb.create_vector(db.as_bytes())),
            tab: Some(fbb.create_vector(line.measurement.as_bytes())),
            tags: Some(fbb.create_vector(&tags)),
            fields: Some(fbb.create_vector(&fields)),
            timestamp: line.timestamp,
        };

        point_offsets.push(Point::create(&mut fbb, &point_args));
    }

    let fbb_db = fbb.create_vector(db.as_bytes());
    let points_raw = fbb.create_vector(&point_offsets);
    let points = Points::create(
        &mut fbb,
        &PointsArgs {
            db: Some(fbb_db),
            points: Some(points_raw),
        },
    );
    fbb.finish(points, None);
    fbb.finished_data().to_vec()
}
