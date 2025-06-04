DEALS_DEPENDENT_ENTITIES_CONFIG = {
    "usuarios_dependentes_deals": {
        "table_name": "usuarios",
        "timestamp_columns": ["created", "modified"]
    },
    "pessoas_dependentes_deals": {
        "table_name": "pessoas",
        "timestamp_columns": ["add_time", "update_time"]
    },
    "etapas_dependentes_deals": {
        "table_name": "etapas_funil",
        "timestamp_columns": ["add_time", "update_time"]
    },
    "pipelines_dependentes_deals": {
        "table_name": "pipelines",
        "timestamp_columns": ["add_time", "update_time"]
    },
    "organizacoes_dependentes_deals": {
        "table_name": "organizacoes",
        "timestamp_columns": ["add_time", "update_time"]
    }
}

PERSONS_DEPENDENT_ENTITIES_CONFIG = {
    "usuarios_dependentes_persons": {
        "table_name": "usuarios",
        "timestamp_columns": ["created", "modified"] 
    },
    "organizacoes_dependentes_persons": {
        "table_name": "organizacoes",
        "timestamp_columns": ["add_time", "update_time"]
    }
}

ORGANIZATIONS_DEPENDENT_ENTITIES_CONFIG = {
    "usuarios_dependentes_organizations": {
        "table_name": "usuarios",
        "timestamp_columns": ["created", "modified"]
    }
}

ACTIVITIES_DEPENDENT_ENTITIES_CONFIG = {
    "usuarios_dependentes_activities": {
        "table_name": "usuarios",
        "timestamp_columns": ["created", "modified"]
    },
    "pessoas_dependentes_activities": {
        "table_name": "pessoas",
        "timestamp_columns": ["add_time", "update_time"]
    },
    "organizacoes_dependentes_activities": {
        "table_name": "organizacoes",
        "timestamp_columns": ["add_time", "update_time"]
    }
}
