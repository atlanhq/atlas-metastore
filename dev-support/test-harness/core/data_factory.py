"""Entity and typedef payload builders for test data."""

import time
import uuid

TIMESTAMP = int(time.time())
PREFIX = f"test-harness-{TIMESTAMP}"


def unique_qn(prefix="th"):
    return f"{PREFIX}/{prefix}-{uuid.uuid4().hex[:8]}"


def unique_name(prefix="th"):
    return f"{prefix}-{TIMESTAMP}-{uuid.uuid4().hex[:6]}"


def unique_type_name(prefix="Th"):
    """Generate a unique name safe for typedef names (alphanumeric + underscore only)."""
    return f"{prefix}_{TIMESTAMP}_{uuid.uuid4().hex[:6]}"


# ---- TypeDef builders ----

def build_enum_def(name=None, elements=None):
    name = name or unique_type_name("TestEnum")
    elements = elements or [
        {"value": "VAL_A", "ordinal": 0},
        {"value": "VAL_B", "ordinal": 1},
        {"value": "VAL_C", "ordinal": 2},
    ]
    return {
        "name": name,
        "displayName": name,
        "description": f"Test enum {name}",
        "typeVersion": "1.0",
        "elementDefs": elements,
    }


def build_classification_def(name=None, super_types=None, attribute_defs=None):
    name = name or unique_type_name("TestClassification")
    return {
        "name": name,
        "displayName": name,
        "description": f"Test classification {name}",
        "typeVersion": "1.0",
        "superTypes": super_types or [],
        "attributeDefs": attribute_defs or [],
    }


def build_struct_def(name=None, attribute_defs=None):
    name = name or unique_type_name("TestStruct")
    return {
        "name": name,
        "displayName": name,
        "description": f"Test struct {name}",
        "typeVersion": "1.0",
        "attributeDefs": attribute_defs or [
            {
                "name": "field1",
                "typeName": "string",
                "isOptional": True,
                "cardinality": "SINGLE",
                "isUnique": False,
                "isIndexable": False,
            }
        ],
    }


def build_entity_def(name=None, super_types=None, attribute_defs=None):
    name = name or unique_type_name("TestEntityType")
    return {
        "name": name,
        "displayName": name,
        "description": f"Test entity type {name}",
        "typeVersion": "1.0",
        "superTypes": super_types or ["DataSet"],
        "attributeDefs": attribute_defs or [
            {
                "name": "testAttr",
                "typeName": "string",
                "isOptional": True,
                "cardinality": "SINGLE",
                "isUnique": False,
                "isIndexable": True,
            }
        ],
    }


def build_business_metadata_def(name=None, attribute_defs=None):
    name = name or unique_type_name("TestBM")
    return {
        "name": name,
        "displayName": name,
        "description": f"Test business metadata {name}",
        "typeVersion": "1.0",
        "attributeDefs": attribute_defs or [
            {
                "name": "bmField1",
                "displayName": "bmField1",
                "typeName": "string",
                "isOptional": True,
                "cardinality": "SINGLE",
                "isUnique": False,
                "isIndexable": True,
                "options": {
                    "applicableEntityTypes": "[\"DataSet\"]",
                    "maxStrLength": "50",
                },
            }
        ],
    }


def build_relationship_def(name=None, end1_type="DataSet", end2_type="DataSet"):
    name = name or unique_type_name("TestRelDef")
    return {
        "name": name,
        "description": f"Test relationship {name}",
        "typeVersion": "1.0",
        "relationshipCategory": "ASSOCIATION",
        "propagateTags": "NONE",
        "endDef1": {
            "type": end1_type,
            "name": f"{name}_end1",
            "isContainer": False,
            "cardinality": "SET",
        },
        "endDef2": {
            "type": end2_type,
            "name": f"{name}_end2",
            "isContainer": False,
            "cardinality": "SET",
        },
    }


# ---- Entity builders ----

def build_dataset_entity(qn=None, name=None, type_name="DataSet", extra_attrs=None):
    qn = qn or unique_qn("dataset")
    name = name or unique_name("dataset")
    attrs = {"qualifiedName": qn, "name": name}
    if extra_attrs:
        attrs.update(extra_attrs)
    return {
        "typeName": type_name,
        "attributes": attrs,
    }


def build_process_entity(qn=None, name=None, inputs=None, outputs=None):
    qn = qn or unique_qn("process")
    name = name or unique_name("process")
    attrs = {"qualifiedName": qn, "name": name}
    if inputs:
        attrs["inputs"] = inputs
    if outputs:
        attrs["outputs"] = outputs
    return {
        "typeName": "Process",
        "attributes": attrs,
    }


def build_glossary(name=None):
    name = name or unique_name("glossary")
    return {
        "typeName": "AtlasGlossary",
        "attributes": {
            "qualifiedName": unique_qn("glossary"),
            "name": name,
            "shortDescription": f"Test glossary {name}",
        },
    }


def build_glossary_term(glossary_guid, name=None):
    name = name or unique_name("term")
    return {
        "typeName": "AtlasGlossaryTerm",
        "attributes": {
            "qualifiedName": unique_qn("term"),
            "name": name,
            "shortDescription": f"Test term {name}",
        },
        "relationshipAttributes": {
            "anchor": {"guid": glossary_guid, "typeName": "AtlasGlossary"},
        },
    }


def build_glossary_category(glossary_guid, name=None, parent_category_guid=None):
    name = name or unique_name("category")
    rel_attrs = {
        "anchor": {"guid": glossary_guid, "typeName": "AtlasGlossary"},
    }
    if parent_category_guid:
        rel_attrs["parentCategory"] = {
            "guid": parent_category_guid,
            "typeName": "AtlasGlossaryCategory",
        }
    return {
        "typeName": "AtlasGlossaryCategory",
        "attributes": {
            "qualifiedName": unique_qn("category"),
            "name": name,
            "shortDescription": f"Test category {name}",
        },
        "relationshipAttributes": rel_attrs,
    }


# ---- Data Mesh entity builders ----

def build_domain_entity(name=None, parent_domain_qn=None):
    """DataDomain entity. QN is auto-generated by preprocessor."""
    name = name or unique_name("domain")
    attrs = {"qualifiedName": unique_qn("domain"), "name": name}
    if parent_domain_qn:
        attrs["parentDomainQualifiedName"] = parent_domain_qn
    return {
        "typeName": "DataDomain",
        "attributes": attrs,
    }


def build_data_product_entity(name=None, domain_guid=None):
    """DataProduct entity linked to a domain.

    dataProductAssetsDSL is mandatory (DataProductPreProcessor validates it).
    We use a narrow DSL that matches nothing to avoid scanning all assets.
    """
    import json
    name = name or unique_name("product")
    # DSL that matches no real assets (fake GUID filter)
    assets_dsl = json.dumps({
        "query": {"bool": {"must": [
            {"term": {"__guid": "00000000-0000-0000-0000-000000000000"}},
        ]}}
    })
    attrs = {
        "qualifiedName": unique_qn("product"),
        "name": name,
        "dataProductAssetsDSL": assets_dsl,
    }
    entity = {
        "typeName": "DataProduct",
        "attributes": attrs,
    }
    if domain_guid:
        entity["relationshipAttributes"] = {
            "dataDomain": {"guid": domain_guid, "typeName": "DataDomain"},
        }
    return entity


# ---- Persona / Purpose / AuthPolicy builders ----

def build_persona_entity(name=None):
    """Persona entity. QN auto-generated by preprocessor."""
    name = name or unique_name("persona")
    return {
        "typeName": "Persona",
        "attributes": {
            "qualifiedName": unique_qn("persona"),
            "name": name,
            "description": f"Test persona {name}",
        },
    }


def build_purpose_entity(name=None):
    """Purpose entity."""
    name = name or unique_name("purpose")
    return {
        "typeName": "Purpose",
        "attributes": {
            "qualifiedName": unique_qn("purpose"),
            "name": name,
            "description": f"Test purpose {name}",
        },
    }


def build_auth_policy_entity(persona_guid, name=None):
    """AuthPolicy linked to a Persona."""
    name = name or unique_name("policy")
    return {
        "typeName": "AuthPolicy",
        "attributes": {
            "qualifiedName": unique_qn("policy"),
            "name": name,
            "policyType": "allow",
            "policyCategory": "persona",
            "policyResourceCategory": "ENTITY",
            "policyActions": ["entity-read"],
            "policyResources": ["entity-type:DataSet"],
        },
        "relationshipAttributes": {
            "accessControl": {"guid": persona_guid, "typeName": "Persona"},
        },
    }


# ---- Multi-attribute BM typedef builder ----

def build_multi_attr_business_metadata_def(name=None):
    """BM def with string + int + boolean attributes."""
    name = name or unique_type_name("TestMultiBM")
    return {
        "name": name,
        "displayName": name,
        "description": f"Multi-attribute BM {name}",
        "typeVersion": "1.0",
        "attributeDefs": [
            {
                "name": "bmStrField",
                "displayName": "bmStrField",
                "typeName": "string",
                "isOptional": True,
                "cardinality": "SINGLE",
                "isUnique": False,
                "isIndexable": True,
                "options": {
                    "applicableEntityTypes": "[\"DataSet\"]",
                    "maxStrLength": "50",
                },
            },
            {
                "name": "bmIntField",
                "displayName": "bmIntField",
                "typeName": "int",
                "isOptional": True,
                "cardinality": "SINGLE",
                "isUnique": False,
                "isIndexable": True,
                "options": {
                    "applicableEntityTypes": "[\"DataSet\"]",
                },
            },
            {
                "name": "bmBoolField",
                "displayName": "bmBoolField",
                "typeName": "boolean",
                "isOptional": True,
                "cardinality": "SINGLE",
                "isUnique": False,
                "isIndexable": True,
                "options": {
                    "applicableEntityTypes": "[\"DataSet\"]",
                },
            },
        ],
    }
