from recipe import Recipe, Field, Datasource, AttributeMatcher, Subject, Match_Rule, LatestValueField, Dataset

subjects = Subject(subject_type_label='airQualityControl', provider_label='erg.kcl.ac.uk')
datasources = Datasource(importer_class='uk.org.tombolo.importer.lac.LAQNImporter', datasource_id='airQualityControl')

attribute_matcher = AttributeMatcher(provider='erg.kcl.ac.uk', label='NO2 40 ug/m3 as an annual me')
lvf = LatestValueField(attribute_matcher=attribute_matcher, label='Anual NO2')

dataset = Dataset(subjects=[subjects], datasources=[datasources], fields=[lvf])
recipe = Recipe(dataset=dataset)
recipe.build_recipe(output_location='Desktop/london-no2.json')
