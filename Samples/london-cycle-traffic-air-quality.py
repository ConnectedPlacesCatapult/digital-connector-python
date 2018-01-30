from os import path, pardir
import sys
sys.path.append(path.join(path.dirname(path.realpath(__file__)), pardir))


from recipe import Recipe, Dataset, Subject, Match_Rule, Datasource, GeographicAggregationField, LatestValueField, AttributeMatcher, ArithmeticField

match_rule = Match_Rule(attribute_to_match_on='label', pattern='E090%')
main_subject = Subject(subject_type_label='localAuthority', provider_label='uk.gov.ons', match_rule=match_rule)

m_datasource_2 = Datasource(importer_class='uk.org.tombolo.importer.ons.OaImporter', datasource_id='localAuthority')
m_datasource_3 = Datasource(importer_class='uk.org.tombolo.importer.dft.TrafficCountImporter', datasource_id='trafficCounts', geography_scope=['London'])
m_datasource_4 = Datasource(importer_class='uk.org.tombolo.importer.lac.LAQNImporter', datasource_id='airQualityControl')


pf_subject = Subject(provider_label='erg.kcl.ac.uk', subject_type_label='airQualityControl')
attribute = AttributeMatcher(provider='erg.kcl.ac.uk', label='NO2 40 ug/m3 as an annual me')
l_v_f = LatestValueField(attribute_matcher=attribute)
parent_field = GeographicAggregationField(subject=pf_subject, label='NitrogenDioxide', function='mean', field=l_v_f)

a_sub = Subject(provider_label='uk.gov.dft', subject_type_label='trafficCounter')
attr = AttributeMatcher(provider='uk.gov.dft', label='CountPedalCycles')
sub_field_latest = LatestValueField(attribute_matcher=attr)
field_1 = GeographicAggregationField(label='BicycleCount', subject=a_sub, function='sum', field=sub_field_latest)
attr_2 = AttributeMatcher(provider='uk.gov.dft', label='CountCarsTaxis')
sub_field_latest_2 = LatestValueField(attribute_matcher=attr_2)
field_2 = GeographicAggregationField(label='CarCount', subject=a_sub, function='sum', field=sub_field_latest_2)
arithmetic_field = ArithmeticField(label='BicycleFraction', operation='div', operation_on_field_1=field_1, operation_on_field_2=field_2)

dataset = Dataset(subjects=[main_subject], datasources=[m_datasource_2, m_datasource_3, m_datasource_4], 
                fields=[parent_field, arithmetic_field])
recipe = Recipe(dataset=dataset)
recipe.build_recipe(output_location='Desktop/london-cycle-traffic-air-quality.json')
recipe.run_recipe(tombolo_path='Desktop/UptodateProject/TomboloDigitalConnector', output_path='Desktop/london-cycle-traffic-air-quality.geojson')
                
