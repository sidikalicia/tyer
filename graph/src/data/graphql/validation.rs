use graphql_parser::schema::*;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Strings(Vec<String>);

impl fmt::Display for Strings {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let s = (&self.0).join(", ");
        write!(f, "{}", s)
    }
}

#[derive(Debug, Fail, PartialEq, Eq)]
pub enum SchemaValidationError {
    #[fail(display = "Interface {} not defined", _0)]
    UndefinedInterface(String),

    #[fail(display = "@entity directive missing on the following types: {}", _0)]
    EntityDirectivesMissing(Strings),

    #[fail(
        display = "Type {}, field {}: directive `{}` has missing 
                      or wrong type arguments",
        _0, _1, _2
    )]
    BadDirective(String, String, String), // (type, field, directive)

    #[fail(
        display = "Type {}, field {}: `@derivedFrom` must applied to a simple list type",
        _0, _1
    )]
    DerivedFromOnWrongType(String, String), // (type, field)

    #[fail(
        display = "Type {}, field {}: `@derivedFrom(field: \"{}\")` directive \
                   on field of type {} but that type does not contain a field \"{}\"",
        _0, _1, _2, _3, _2
    )]
    DerivedFromMissingField(String, String, String, String), // (type, field, target_field, field_type)

    #[fail(
        display = "Type {}, field {}: On the `@derivedFrom(field: \"{}\")` directive, \
                   the type `{}` of field \"{}\" does not match the expected type `{}`",
        _0, _1, _2, _3, _2, _0
    )]
    DerivedFromWrongTargetType(String, String, String, String), // (type, field, target_field, target_field_type)
}

/// Validates whether a GraphQL schema is compatible with The Graph.
pub(crate) fn validate_schema(schema: &Document) -> Result<(), SchemaValidationError> {
    validate_entity_directive(schema)?;
    validate_derived_from(schema)
}

/// Validates whether all object types in the schema are declared with an @entity directive.
fn validate_entity_directive(schema: &Document) -> Result<(), SchemaValidationError> {
    use self::SchemaValidationError::*;

    let types_without_entity_directive = get_object_type_definitions(schema)
        .iter()
        .filter(|t| get_object_type_directive(t, String::from("entity")).is_none())
        .map(|t| t.name.to_owned())
        .collect::<Vec<_>>();

    if types_without_entity_directive.is_empty() {
        Ok(())
    } else {
        return Err(EntityDirectivesMissing(Strings(
            types_without_entity_directive,
        )));
    }
}

// Validate that any `derivedFrom` directive has a `field` argument, is applied
// to a simple list type and that `field` exists and has the expected type in
// the target type.
//
// TODO: validate that interfaces don't have `derivedFrom`
fn validate_derived_from(schema: &Document) -> Result<(), SchemaValidationError> {
    use self::SchemaValidationError::*;

    for object in get_object_type_definitions(schema) {
        for (field, derived_from) in object
            .fields
            .iter()
            .filter_map(|f| get_derived_from_directive(&f).map(|d| (f, d)))
        {
            // Validate the `field` argument.
            let field_arg = derived_from.arguments.iter().find(|(n, _)| n == "field").map(|(_, v)| v);
            let field_arg = match field_arg {
                Some(Value::String(field_arg)) => field_arg,
                None => {
                    return Err(BadDirective(
                        object.name.clone(),
                        field.name.clone(),
                        "@derivedFrom".to_owned(),
                    ));
                }
            };

            // Must be applied to a simple list type.
            let target_type = match ignore_nullability(&field.field_type) {
                Type::ListType(t) => match ignore_nullability(&*t) {
                    Type::NamedType(s) => s,

                    // Nested list
                    _ => {
                        return Err(DerivedFromOnWrongType(
                            object.name.clone(),
                            field.name.clone(),
                        ));
                    }
                },

                // Named type
                _ => {
                    return Err(DerivedFromOnWrongType(
                        object.name.clone(),
                        field.name.clone(),
                    ));
                }
            };

            // Target type must contain a field with name `field_arg` and same type as this object.
            let target_type_fields =
                get_type_definitions(schema).find_map(|typedef| match typedef {
                    TypeDefinition::Object(t) if t.name == target_type.name => Some(t.fields),
                    TypeDefinition::Interface(t) if t.name == target_type.name => Some(t.fields),
                    _ => None,
                });

            let target_field = target_type_fields
                .and_then(|fields| fields.iter().find(|f| *f.name == field_arg));

            match target_field {
                Some(target_field) => match ignore_nullability(&target_field.field_type) {
                    Type::NamedType(s) if s == &object.name => (),
                    Type::ListType(t) => match ignore_nullability(&*t) {
                        Type::NamedType(s) if s == &object.name => (),
                        _ => {
                            return Err(DerivedFromWrongTargetType(
                                object.name.clone(),
                                field.name.clone(),
                                field_arg,
                                target_field.field_type.to_string(),
                            ));
                        }
                    },
                    _ => {
                        return Err(DerivedFromWrongTargetType(
                            object.name.clone(),
                            field.name.clone(),
                            field_arg,
                            target_field.field_type.to_string(),
                        ));
                    }
                },
                None => {
                    return Err(DerivedFromMissingField(
                        object.name.clone(),
                        field.name.clone(),
                        field_arg,
                        target_type.clone(),
                    ));
                }
            }
        }
    }

    Ok(())
}

/// Returns all object type definitions in the schema.
pub fn get_object_type_definitions(schema: &Document) -> Vec<&ObjectType> {
    schema
        .definitions
        .iter()
        .filter_map(|d| match d {
            Definition::TypeDefinition(TypeDefinition::Object(t)) => Some(t),
            _ => None,
        })
        .collect()
}

/// Returns all interface definitions in the schema.
pub fn get_interface_type_definitions(schema: &Document) -> Vec<&InterfaceType> {
    schema
        .definitions
        .iter()
        .filter_map(|d| match d {
            Definition::TypeDefinition(TypeDefinition::Interface(t)) => Some(t),
            _ => None,
        })
        .collect()
}

/// Looks up a directive in a object type, if it is provided.
pub fn get_object_type_directive(object_type: &ObjectType, name: Name) -> Option<&Directive> {
    object_type
        .directives
        .iter()
        .find(|directive| directive.name == name)
}

fn get_derived_from_directive(field_definition: &Field) -> Option<&Directive> {
    field_definition
        .directives
        .iter()
        .find(|directive| directive.name == Name::from("derivedFrom"))
}

/// Returns all type definitions in the schema.
fn get_type_definitions(schema: &Document) -> impl Iterator<Item = &TypeDefinition> {
    schema.definitions.iter().filter_map(|d| match d {
        Definition::TypeDefinition(typedef) => Some(typedef),
        _ => None,
    })
}

fn ignore_nullability(t: &Type) -> &Type {
    match t {
        Type::NonNullType(t) => &*t,
        _ => t,
    }
}
