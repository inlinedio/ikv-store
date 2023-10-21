use std::collections::HashMap;

use super::field::Field;
use yaml_rust::YamlLoader;

/// Parse yaml string into vector of Field structs. Order of field structs is undefined.
pub fn load_yaml_schema(schema_str: &str) -> Vec<Field> {
    let schema = &YamlLoader::load_from_str(schema_str).unwrap()[0];
    let mut fields = vec![];

    let document = &schema["document"];
    for field in document.as_vec().unwrap() {
        let field_name = field["name"]
            .as_str()
            .expect("`name` is a required attribute");
        let field_id = field["id"].as_i64().expect("`id` is a required attribute");
        let field_type = field["type"]
            .as_str()
            .expect("`type` is a required attribute");
        let field = Field::new(
            field_name.to_string(),
            field_id as u16,
            field_type.to_string().try_into().unwrap(),
        );

        fields.push(field);
    }

    fields
}

pub fn sort_by_field_id(fields: &mut [Field]) {
    fields.sort_by(|f1, f2| f1.id().cmp(&f2.id()));
}

pub fn to_map(fields: &[Field]) -> HashMap<String, Field> {
    let mut result = HashMap::with_capacity(fields.len());
    for field in fields {
        result.insert(field.name().to_string(), field.clone());
    }

    result
}

#[cfg(test)]
mod tests {
    use yaml_rust::YamlLoader;

    use super::load_yaml_schema;

    #[test]
    fn illustration() {
        let yaml_str = "
        document:
          - name: field0
            id: 0
            type: i32
          - name: field1
            id: 1
            type: bytes
          - name: field2
            id: 2
            type: string";

        let yaml = &YamlLoader::load_from_str(yaml_str).unwrap()[0];
        let fields = yaml["document"].as_vec().unwrap();
        assert_eq!(fields.len(), 3);

        assert_eq!(yaml["document"][0]["name"].as_str().unwrap(), "field0");
        assert_eq!(yaml["document"][0]["id"].as_i64().unwrap(), 0 as i64);
        assert_eq!(yaml["document"][0]["type"].as_str().unwrap(), "i32");

        assert_eq!(yaml["document"][1]["id"].as_i64().unwrap(), 1 as i64);
        assert_eq!(yaml["document"][1]["type"].as_str().unwrap(), "bytes");

        assert_eq!(yaml["document"][2]["id"].as_i64().unwrap(), 2 as i64);
        assert_eq!(yaml["document"][2]["type"].as_str().unwrap(), "string");
    }

    #[test]
    fn parse() {
        let yaml_str = "
        document:
        - name: firstname
          id: 0
          type: string
        - name: age
          id: 1
          type: i32
        - name: profile
          id: 2
          type: bytes";

        let fields = load_yaml_schema(yaml_str);
        assert_eq!(fields.len(), 3);

        assert_eq!(fields[0].name(), "firstname");
        assert_eq!(fields[0].id(), 0);
        assert_eq!(fields[0].value_len(), None);

        assert_eq!(fields[1].name(), "age");
        assert_eq!(fields[1].id(), 1);
        assert_eq!(fields[1].value_len().unwrap(), 4 as usize);

        assert_eq!(fields[2].name(), "profile");
        assert_eq!(fields[2].id(), 2);
        assert_eq!(fields[2].value_len(), None);
    }
}
