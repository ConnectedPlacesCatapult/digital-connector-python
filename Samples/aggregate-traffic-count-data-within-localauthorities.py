from os import path, pardir
import sys
sys.path.append(path.join(path.dirname(path.realpath(__file__)), pardir))

from recipe import Dataset, Subject, AttributeMatcher, GeographicAggregationField, LatestValueField, Match_Rule, Datasource, Recipe

match_rule = Match_Rule(attribute_to_match_on='label', pattern='E090%')
subject = Subject(subject_type_label='localAuthority', provider_label='uk.gov.ons', match_rule=match_rule)

datasource_1 = Datasource(importer_class='uk.org.tombolo.importer.ons.OaImporter', datasource_id='localAuthority')
datasource_2 = Datasource(importer_class='uk.org.tombolo.importer.dft.TrafficCountImporter', 
                        datasource_id= 'trafficCounts', geography_scope=["London"])

attribute_matcher = AttributeMatcher(provider='uk.gov.dft', label='CountPedalCycles')
field = LatestValueField(attribute_matcher=attribute_matcher, label='CountPedalCycles')

subject_2 = Subject(subject_type_label='trafficCounter', provider_label='uk.gov.dft')
geo_agg_field = GeographicAggregationField(subject=subject_2, field=field, function='sum', label='SumCountPedalCycles')

dataset = Dataset(subjects=[subject], datasources=[datasource_1, datasource_2], fields=[geo_agg_field])
_recipe = Recipe(dataset=dataset)
_recipe.build_recipe(output_location='Desktop/aggregate-traffic-count-data-within-localauthorities.json')
_recipe.run_recipe(tombolo_path='Desktop/UptodateProject/TomboloDigitalConnector', output_path='Desktop/aggregate-traffic-count-data-within-localauthorities.geojson')


