from pedsnetdcc.age_transform import AgeTransform                   # noqa
from pedsnetdcc.concept_name_transform import ConceptNameTransform  # noqa
from pedsnetdcc.site_name_transform import SiteNameTransform        # noqa
from pedsnetdcc.id_mapping_transform import IDMappingTransform      # noqa

TRANSFORMS = (AgeTransform, ConceptNameTransform, SiteNameTransform,
              IDMappingTransform)
