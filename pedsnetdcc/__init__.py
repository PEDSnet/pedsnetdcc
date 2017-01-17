import os

serial = os.environ.get('BUILD_NUM') or '0'
sha = os.environ.get('COMMIT_SHA1') or '0'
sha = sha[0:8]

__version_info__ = {
    'major': 0,
    'minor': 0,
    'micro': 1,
    'releaselevel': 'alpha',
    'serial': serial,
    'sha': sha
}


def get_version(short=False):
    assert __version_info__['releaselevel'] in ('alpha', 'beta', 'final')
    vers = ['%(major)i.%(minor)i.%(micro)i' % __version_info__, ]
    if __version_info__['releaselevel'] != 'final' and not short:
        __version_info__['lvlchar'] = __version_info__['releaselevel'][0]
        vers.append('%(lvlchar)s%(serial)s+%(sha)s' % __version_info__)
    return ''.join(vers)

__version__ = get_version()

# TODO: This should be a function that takes the model version, since it
# may change over time.
VOCAB_TABLES = (
    'vocabulary',
    'concept',
    'concept_ancestor',
    'concept_class',
    'concept_relationship',
    'concept_synonym',
    'domain',
    'drug_strength',
    'relationship',
    'source_to_concept_map'
)

# TODO: Generate this map dynamically at runtime from the distinct
# fact_relationship.domain_concept_id_{1,2} values and the domain table.
FACT_RELATIONSHIP_DOMAINS = {
    'observation': 27,
    'measurement': 21,
    'visit_occurrence': 8
}

SITE_ROOT = '/data/site_data'
SITES = ('chop', 'colorado', 'nationwide', 'nemours', 'seattle', 'stlouis')
BACKUP_ROOT = '/mnt/isilon/pedsnet/archives/ftp/'

CONSISTENT_ID_MAP_TABLES = (
    'person',
    'visit_occurrence'
)

ID_MAP_TABLES = CONSISTENT_ID_MAP_TABLES + (
    'care_site',
    'condition_occurrence',
    'death',
    'drug_exposure',
    'location',
    'measurement',
    'measurement_organism',
    'observation',
    'observation_period',
    'procedure_occurrence',
    'provider',
    'visit_payer'
)

_dms_var = 'PEDSNETDCC_DMS_URL'
if _dms_var in os.environ:
    DATA_MODELS_SERVICE = os.environ[_dms_var]
else:
    DATA_MODELS_SERVICE = 'https://data-models-service.research.chop.edu/'

__all__ = (__version__, VOCAB_TABLES, DATA_MODELS_SERVICE,
           FACT_RELATIONSHIP_DOMAINS)
