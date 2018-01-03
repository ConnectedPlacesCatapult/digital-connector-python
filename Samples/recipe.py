import json
from pathlib import Path

base_dir = str(Path.home())

class Recipe(object):
    def __init__(self, dataset, exporter='uk.org.tombolo.exporter.GeoJsonExporter', timestamp=False):
        if not isinstance(dataset, Dataset):
            raise TypeError('dataset should be of type Dataset')

        self.dataset = dataset
        self.exporter = exporter
        self.timeStamp = timestamp

    def build_recipe(self, output_location):
        recipe = json.dumps(self, indent=2, default=lambda a: a.__dict__)
        with open(base_dir + '/' + output_location, 'w') as recipe_file:
            recipe_file.write(recipe)
        print(recipe)

class Dataset(object):
    def __init__(self, subjects, datasources, fields):
        all_same_type(list, [subjects, datasources, fields], 'subjects, datasources and fields should be list')
        all_same_type(Subject, subjects, 'subjects should be of type Subject')
        all_same_type(Datasource, datasources, 'datasources should be of type Datasource')
        all_same_type(Field, fields, 'fields should be a type Field')

        self.subjects = subjects
        self.datasources = datasources
        self.fields = fields

class AttributeMatcher(object):
    def __init__(self, provider, label, values=[]):
        self.provider = provider
        self.label = label
        is_list_object(values, 'values should be of type list')
        if values:
            self.values = values

class Datasource:
    '''
    This class defines what a datasource object could take as a parameter
    '''
    def __init__(self, importer_class, datasource_id, geography_scope=[], temporal_scope=[], local_data=[]):
        '''
        Init method that takes 4 arguments out of which two are optional
        '''
        self.importerClass = importer_class
        self.datasourceId = datasource_id
        if geography_scope:
            self.geographyScope = geography_scope
        if temporal_scope:
            self.temporalScope = temporal_scope
        if local_data:
            self.localData = local_data

class Subject:
    def __init__(self, subject_type_label, provider_label, match_rule=None, geo_match_rule=None):
        self.subjectType = subject_type_label
        self.provider = provider_label
        if match_rule is not None:
            is_of_type(Match_Rule, match_rule, 'match_rule must be of type Match_Rule')
            self.matchRule = match_rule

        if geo_match_rule is not None:
            is_of_type(Geo_Match_Rule, geo_match_rule, 'geo_match_rule must be of type Geo_Match_Rule')
            self.geoMatchRule = geo_match_rule

class Geo_Match_Rule(object):
    def __init__(self, geo_relation, subjects):
        self.geoRelation = geo_relation
        is_list_object(subjects, 'subjects should be list of subjects')
        all_same_type(Subject, subjects, 'subjects should be a list of subjects')
        self.subjects = subjects

class Match_Rule:
    def __init__(self, attribute_to_match_on, pattern):
        self.attribute = attribute_to_match_on
        self.pattern = pattern


class Field(object):
    def __init__(self, field_class, label):
        self.fieldClass = field_class
        if label is not None:
            self.label = label

'''
below are the field class from transformation package
'''
package_name_transformation = 'uk.org.tombolo.field.transformation.'
class AreaField(Field):
    def __init__(self, target_crs_code, label=None):
        is_of_type(int, target_crs_code, 'target_crs_code should be of type int')
        
        super().__init__(field_class=package_name_transformation + 'AreaField', label=label)
        self.targetCRSCode = target_crs_code

class ArithmeticField(Field):
    def __init__(self, operation, operation_on_field_1, operation_on_field_2, label=None):
        all_same_type(Field, [operation_on_field_1, operation_on_field_2], 
                        'operation_on_field_1 and operation_on_field_2 should be of classes that are inherited' \
                        'from Field base class')

        super().__init__(field_class=package_name_transformation + 'ArithmeticField', label=label)
        self.operation = operation
        self.field1 = operation_on_field_1
        self.field2 = operation_on_field_2

class DescriptiveStatisticsField(Field):
    def __init__(self, statistic, fields, label=None):
        is_list_object(fields, 'fields should of type list of Field')
        all_same_type(Field, fields, 'fields should be of type Field')

        super().__init__(field_class=package_name_transformation + 'DescriptiveStatisticsField', label=label)
        self.statistic = statistic
        self.fields = fields

class FieldValueSumField(Field):
    def __init__(self, name, fields, label=None):
        is_list_object(fields, 'fields should be an instance of list')
        all_same_type(Field, fields, 'fields should be of type Field')

        super().__init__(field_class=package_name_transformation + 'FieldValueSumField', label=label)
        self.name = name
        self.fields = fields

class FractionOfTotalField(Field):
    def __init__(self, dividend_attributes, divisor_attribute, label=None):
        is_list_object(dividend_attributes)
        all_same_type(AttributeMatcher, dividend_attributes, 'dividend_attributes must be of type AttributeMatcher')
        is_of_type(AttributeMatcher, divisor_attribute, 'divisor_attribute should be of type AttributeMatcher')

        super().__init__(field_class=package_name_transformation + 'FractionOfTotalField', label=label)
        self.dividendAttributes = dividend_attributes
        self.divisorAttribute = divisor_attribute

class LinearCombinationField(Field):
    def __init__(self, scalars, fields, label=None):
        is_list_object(fields)
        all_same_type(Field, fields, 'fields Should be of type Field')

        is_list_object(scalars)
        all_same_type(float, scalars, 'scalars should be of type float')

        super().__init__(field_class=package_name_transformation + 'LinearCombinationField', label=label)
        self.scalars = scalars
        self.fields = fields

class ListArithmeticField(Field):
    def __init__(self, operation, fields, label=None):
        is_list_object(fields)
        all_same_type(Field, fields, 'fields should be of type Field')

        super().__init__(field_class=package_name_transformation + 'ListArithmeticField', label=label)
        self.operation = operation
        self.fields = fields

class PercentilesField(Field):
    def __init__(self, name, field, subjects, percentile_count, inverse, label=None):
        is_of_type(Field, field, 'field should be of type Field')
        is_list_object(subjects)
        all_same_type(Subject, subjects, 'subjects should be of type Subject')
        is_of_type(int, percentile_count)
        is_of_type(bool, inverse)

        super().__init__(field_class=package_name_transformation + 'PercentilesField', label=label)
        self.valueField = field
        self.normalizationSubjects = subjects
        self.percentileCount = percentile_count
        self.inverse = inverse


'''
below are the field classes from value package
'''
package_name_value = 'uk.org.tombolo.field.value.'
class BasicValueField(Field):
    def __init__(self, attribute_matcher, field_class, label=None):
        super().__init__(field_class=field_class, label=label)
        if attribute_matcher is not None:
            is_of_type(AttributeMatcher, attribute_matcher, 'attribute_matcher should be of type AttributeMatcher')
            self.attribute = attribute_matcher

class FixedAnnotationField(Field):
    def __init__(self, value, label=None):
        super().__init__(field_class=package_name_value + 'FixedAnnotationField', label=label)
        self.value = value

class FixedValueField(BasicValueField):
    def __init__(self, attribute_matcher=None, label=None):
        super().__init__(attribute_matcher=attribute_matcher, field_class=package_name_value + 'FixedValueField', 
                        label=label)

class LatestValueField(BasicValueField):
    def __init__(self, attribute_matcher=None, label=None):
        super().__init__(attribute_matcher=attribute_matcher, field_class=package_name_value + 'LatestValueField', 
                        label=label)

class SubjectLatitudeField(Field):
    def __init__(self, label=None):
        super().__init__(field_class=package_name_value + 'SubjectLatitudeField', label=label)

class SubjectLongitudeField(Field):
    def __init__(self, label=None):
        super().__init__(field_class=package_name_value + 'SubjectLongitudeField', label=label)

class SubjectNameField(Field):
    def __init__(self, label=None):
        super().__init__(field_class=package_name_value + 'SubjectNameField', label=label)

class TimeseriesField(BasicValueField):
    def __init__(self, label=None, attribute_matcher=None):
        super().__init__(field_class=package_name_value + 'TimeseriesField', label=label, 
                        attribute_matcher=attribute_matcher)


'''
below are the field classes from aggregation package
'''
package_name_aggregation = 'uk.org.tombolo.field.aggregation.'
class BackOffField(Field):
    def __init__(self, fields, label=None):
        is_list_object(fields)
        all_same_type(Field, fields, 'fields should be of type Field')
        
        super().__init__(field_class=package_name_aggregation + 'BackOffField', label=label)
        self.fields = fields

class GeographicAggregationField(Field):
    def __init__(self, subject, function, field, label=None):
        is_of_type(Subject, subject, 'subject should be of type Subject')
        is_of_type(Field, field, 'field should be of type Field')

        super().__init__(field_class=package_name_aggregation + 'GeographicAggregationField', label=label)
        self.subject = subject
        self.function = function
        self.field = field

class MapToContainingSubjectField(Field):
    def __init__(self, subject, field, label=None):
        is_of_type(Subject, subject, 'subject should be of type Subject')
        is_of_type(Field, field, 'field should be of type Field')

        super().__init__(field_class=package_name_aggregation + 'MapToContainingSubjectField', label=label)
        self.subject = subject
        self.field = field

class MapToNearestSubjectField(Field):
    def __init__(self, subject, max_radius, field, label=None):
        is_of_type(Subject, subject, 'subject should be of type Subject')
        is_of_type(Field, field, 'field should be of type Field')
        is_of_type(float, max_radius, 'max_radius should be of type double')

        super().__init__(field_class=package_name_aggregation + 'MapToNearestSubjectField', label=label)
        self.maxRadius = max_radius
        self.subject = subject
        self.field = field

'''
below are the field classes from assertion package
'''
package_name_assertion = 'uk.org.tombolo.field.assertion.'
class AttributeMatcherField(Field):
    def __init__(self, attributes, field, label=None, field_class='AttributeMatcherField'):
        super().__init__(field_class=package_name_assertion + field_class, label=label)

        if attributes is not None:
            is_list_object(attributes)
            all_same_type(AttributeMatcher, attributes, 'all attributes must be of type AttributeMatcher')
            self.attributes = attributes
        
        if field is not None:
            is_of_type(Field, field, 'field should be of type Field')
            self.field = field

class OSMBuiltInAttributeMatcherField(AttributeMatcherField):
    def __init__(self, attributes=None, field=None, label=None):
        super().__init__(attributes=attributes, field=field, label=label, 
                        field_class='OSMBuiltInAttributeMatcherField')

'''
below are the field classes from modelling package
'''
package_name_modelling = 'uk.org.tombolo.field.modelling.'
class BasicModellingField(Field):
    def __init__(self, recipe, datasources, label=None, field_class='BasicModellingField'):
        super().__init__(field_class=package_name_modelling + field_class, label=label)
        self.recipe = recipe
        if datasources is not None:
            is_list_object(datasources)
            all_same_type(Datasource, datasources, 'datasources should be of type Datasource')
            self.datasources = datasources

class SingleValueModellingField(BasicModellingField):
    def __init__(self, recipe, datasources=None, label=None):
        super().__init__(recipe=recipe, datasources=datasources, label=label, 
                        field_class='SingleValueModellingField')


'''
Helper functions
'''
def is_list_object(var, error_msg='Should be an instance of list'):
    if not isinstance(var, list):
        raise TypeError(error_msg)

def all_same_type(class_name, values, error_msg='must be of different type'):
    if not all(isinstance(v, class_name) for v in values):
        raise TypeError(error_msg)

def is_of_type(class_name, value, error_msg='should be of different type'):
    if not isinstance(value, class_name):
        raise TypeError(error_msg)








