DEALS_LOOKUP_MAPPINGS = {
    'negocios': {
        'owner_id': {
            'source': 'usuarios',
            'key': 'id',
            'value_col': 'name',
            'target_col': 'owner_name'
        },
        'creator_user_id': {
            'source': 'usuarios',
            'key': 'id',
            'value_col': 'name',
            'target_col': 'creator_user_name'
        },
        'person_id': {
            'source': 'pessoas',
            'key': 'id',
            'value_col': 'name',
            'target_col': 'person_name'
        },
        'stage_id': {
            'source': 'etapas_funil',
            'key': 'id',
            'value_col': 'name',
            'target_col': 'stage_name'
        },
        'pipeline_id': {
            'source': 'pipelines',
            'key': 'id',
            'value_col': 'name',
            'target_col': 'pipeline_name'
        }
    },
}

LEADS_LOOKUP_MAPPINGS = {
    'leads': {
        'owner_id': {
            'source': 'usuarios',
            'key': 'id',
            'value_col': 'name',
            'target_col': 'owner_name'
        },
        'person_id': {
            'source': 'pessoas',
            'key': 'id',
            'value_col': 'name',
            'target_col': 'person_name'
        },
        'organization_id': {
            'source': 'organizacoes',
            'key': 'id',
            'value_col': 'name',
            'target_col': 'organization_name'
        }
    }
}

ORGANIZATIONS_LOOKUP_MAPPINGS = {
    'organizacoes': {
        'owner_id': {
            'source': 'usuarios',
            'schema': 'public', 
            'key': 'id',
            'value_col': 'name',
            'target_col': 'owner_name'
        }
    }
}

PERSONS_LOOKUP_MAPPINGS = {
    'pessoas': {
        'owner_id': {
            'source': 'usuarios',
            'key': 'id',
            'value_col': 'name',
            'target_col': 'owner_name'
        },
        'org_id': {
            'source': 'organizacoes',
            'key': 'id',
            'value_col': 'name',
            'target_col': 'org_name'
        }
    }
}


STAGES_LOOKUP_MAPPINGS = {
    'etapas_funil': {
        'pipeline_id': {
            'source': 'pipelines',
            'key': 'id',
            'value_col': 'name',
            'target_col': 'pipeline_name'
        }
    }
}

ACTIVITIES_LOOKUP_MAPPINGS = {
    'atividades': {
        'user_id': {
            'source': 'usuarios',
            'key': 'id',
            'value_col': 'name',
            'target_col': 'user_name'
        },
        'person_id': {
            'source': 'pessoas',
            'key': 'id',
            'value_col': 'name',
            'target_col': 'person_name'
        },
        'org_id': {
            'source': 'organizacoes',
            'key': 'id',
            'value_col': 'name',
            'target_col': 'org_name'
        },
        'created_by_user_id': {
            'source': 'usuarios',
            'key': 'id',
            'value_col': 'name',
            'target_col': 'created_by_user_name'
        },
        'assigned_to_user_id': {
            'source': 'usuarios',
            'key': 'id',
            'value_col': 'name',
            'target_col': 'assigned_to_user_name'
        },
        'update_user_id': {
            'source': 'usuarios',
            'key': 'id',
            'value_col': 'name',
            'target_col': 'update_user_name'
        }
    }
}