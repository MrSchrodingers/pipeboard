from typing import Any, Dict
import unicodedata
import re
import logging
import numpy as np

log = logging.getLogger(__name__)

# Regex pré-compilado para performance
_normalize_regex_1 = re.compile(r'[^\w_]+')
_normalize_regex_2 = re.compile(r'_+')

def normalize_column_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    name = name.lower()
    try:
        name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')
    except Exception:
        pass
    name = _normalize_regex_1.sub('_', name)
    name = _normalize_regex_2.sub('_', name).strip('_')
    if name and name[0].isdigit():
        name = '_' + name
    return name or "_invalid_normalized_name"

# Versão vetorizada para uso com pandas
normalize_column_name_vec = np.vectorize(normalize_column_name)

ADDRESS_COMPONENT_SUFFIX_MAP = {
    'street_number': 'numero_da_casa',
    'route': 'nome_da_rua',
    'sublocality': 'distrito_sub_localidade',
    'locality': 'cidade_municipio_vila_localidade',
    'admin_area_level_1': 'estado',
    'admin_area_level_2': 'regiao',
    'country': 'pais',
    'postal_code': 'cep_codigo_postal',
    'latitude': 'latitude',
    'longitude': 'longitude',
}

ADDRESS_INDICATOR_KEYS = {'formatted_address', 'locality', 'country', 'postal_code'}

def flatten_custom_fields(custom_fields: Dict[str, Any], repo_custom_mapping: Dict[str, str]) -> Dict[str, Any]:
    """
    Achata os campos personalizados, tratando campos de endereço de forma especial
    para extrair seus componentes em colunas separadas.
    """
    flat = {}

    for api_key, col_base in repo_custom_mapping.items():
        data = custom_fields.get(api_key)
        flat[col_base] = None

        if isinstance(data, dict):
            is_address = any(k in data for k in ADDRESS_INDICATOR_KEYS)
            if is_address:
                flat[col_base] = data.get('formatted_address') or data.get('value')
                for comp_key, suffix in ADDRESS_COMPONENT_SUFFIX_MAP.items():
                    flat[f"{col_base}_{suffix}"] = data.get(comp_key)
            else:
                flat[col_base] = data.get('value')

        elif data is not None:
            flat[col_base] = data

    return flat
