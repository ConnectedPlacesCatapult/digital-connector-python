from os import path, pardir
import sys
sys.path.append(path.join(path.dirname(path.realpath(__file__)), pardir))

tombolo_path = 'Desktop/TomboloDigitalConnector'
recipe_output_location = 'Desktop/london-no2.json'
model_output = 'Desktop/london-no2.geojson'

from recipe import Recipe, Field, Datasource, AttributeMatcher, Subject, Match_Rule, LatestValueField, Dataset

subjects = Subject(subject_type_label='airQualityControl', provider_label='erg.kcl.ac.uk')
datasources = Datasource(importer_class='uk.org.tombolo.importer.lac.LAQNImporter', datasource_id='airQualityControl')

attribute_matcher = AttributeMatcher(provider='erg.kcl.ac.uk', label='NO2 40 ug/m3 as an annual me')
lvf = LatestValueField(attribute_matcher=attribute_matcher, label='Anual NO2')

dataset = Dataset(subjects=[subjects], datasources=[datasources], fields=[lvf])
dataset.build_and_run(tombolo_path=tombolo_path, model_output_location=model_output, 
                        recipe_console_print=True)
