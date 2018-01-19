from recipe import is_of_type, is_list_object, all_same_type
from py4j.java_gateway import JavaGateway, GatewayParameters, CallbackServerParameters
from pathlib import Path
import threading
import subprocess as sp

home_dir = str(Path.home())
server_started = False
gateway = None

class Provider(object):
    def __init__(self, label, name):
        self._label = label
        self._name = name

    def to_java_provider(self):
        global gateway
        return gateway.jvm.uk.org.tombolo.core.Provider(self._label, self._name)


class Attribute(object):
    def __init__(self, provider, label, description):
        is_of_type(Provider, provider)

        self._provider = provider
        self._label = label
        self._description = description

    def to_java_attribute(self):
        global gateway
        return gateway.jvm.uk.org.tombolo.core.Attribute(self._provider.to_java_provider(), 
                self._label, self._description)

class SubjectType(object):
    def __init__(self, provider, label, name):
        is_of_type(Provider, provider)

        self._provider = provider
        self._label = label
        self._name = name

    def to_java_subject_type(self):
        global gateway
        return gateway.jvm.uk.org.tombolo.core.SubjectType(self._provider.to_java_provider(),
                self._label, self._name)

class Subject(object):
    def __init__(self, subject_type, label, name, shape):
        is_of_type(SubjectType, subject_type)

        self._subject_type = subject_type
        self._label = label
        self._name = name
        self._shape = shape
    
    def to_java_subject(self):
        global gateway
        return gateway.jvm.uk.org.tombolo.core.Subject(self._subject_type.to_java_subject_type(),
            self._label, self._name, self._shape)

class Geometry(object):
    def __init__(self, latitude, longitude):
        self._latitute = latitude
        self._longitute = longitude

    def to_java_geometry(self):
        global gateway
        geometry = gateway.jvm.com.vividsolutions.jts.geom.Geometry
        coordinates = gateway.jvm.com.vividsolutions.jts.geom.Coordinate(gateway.jvm.java.lang.Double.parseDouble(self._longitute), 
                        gateway.jvm.java.lang.Double.parseDouble(self._latitute))
        percision_model = gateway.jvm.com.vividsolutions.jts.geom.PrecisionModel()
        srid = gateway.jvm.int
        srid = 4326
        geo_factory = gateway.jvm.com.vividsolutions.jts.geom.GeometryFactory(percision_model, srid)
        geometry = geo_factory.createPoint(coordinates)
        return geometry

class TimedValue(object):
    def __init__(self, subject, attribute, timestamp, value):
        is_of_type(Subject, subject)
        is_of_type(Attribute, attribute)

        self._subject = subject
        self._attribute = attribute
        self._timestamp = timestamp
        self._value = value

    def to_java_timed_value(self):
        global gateway
        return gateway.jvm.uk.org.tombolo.core.TimedValue(self._subject.to_java_subject(),
            self._attribute.to_java_attribute(), self._timestamp, self._value)

class FixedValue(object):
    def __init__(self, subject, attribute, value):
        is_of_type(Subject, subject)
        is_of_type(Attribute, attribute)

        self._subject = subject
        self._attribute = attribute
        self._value = value

    def to_java_fixed_value(self):
        global gateway
        return gateway.jvm.uk.org.tombolo.core.FixedValue(self._subject.to_java_subject(),
            self._attribute.to_java_attribute(), self._value)
    

class AbstractImporter(object):
    def __init__(self, tombolo_path, print_data=False):
        global gateway
        self._tombolo_path = tombolo_path
        self._print_data = print_data
        self.start_server()
        gateway = self.gateway_obj()

    class Java:
        implements = ["uk.org.tombolo.Py4jServerInterface"]

    def streamData(self, data):
        if self._print_data:
            print(data)
        return data

    def start_server(self):
        global server_started
        if not server_started:
            run_server = RunPy4jServer(tombolo_path=self._tombolo_path)
            run_server.start()
            server_started = True
            import time
            # Giving py4j time to start the sever and start accepting connection
            time.sleep(1)
        else:
            print('Server is already running')

    def gateway_obj(self):
        gateway = JavaGateway()
        return gateway

    def gateway_obj_with_callback(self, python_entry):
        gateway = JavaGateway(callback_server_parameters=CallbackServerParameters(), 
                                    python_server_entry_point=python_entry)
        return gateway

    def download_data(self, url, data_cache_directory='/tmp', prefix='', suffix=''):
        importer = AbstractImporter(self._tombolo_path, print_data=True)
        gateway = self.gateway_obj_with_callback(python_entry=importer)
        gateway.entry_point.downloadData(url, data_cache_directory, prefix, suffix)
        gateway.shutdown()
    
    def save_provider(self, provider):
        global gateway
        is_of_type(Provider, provider)
        gateway.entry_point.saveProvider(provider.to_java_provider())

    def save_attribute(self, attributes):
        global gateway
        is_list_object(attributes)
        all_same_type(Attribute, attributes)

        attr_list = gateway.jvm.java.util.ArrayList()
        for attribute in attributes:
            attr_list.append(attribute.to_java_attribute())
        gateway.entry_point.saveAttributes(attr_list)

    def save_timed_values(self, timed_values):
        global gateway
        is_list_object(timed_values)
        all_same_type(TimedValue, timed_values)

        time_list = gateway.jvm.java.util.ArrayList()
        for value in timed_values:
            time_list.append(value.to_java_timed_value())
        gateway.entry_point.saveAndClearTimedValueBuffer(time_list)


    def save_fixed_values(self, fixed_values):
        global gateway
        is_list_object(fixed_values)
        all_same_type(FixedValue, fixed_values)

        fixed_list = gateway.jvm.java.util.ArrayList()
        for value in fixed_values:
            fixed_list.append(value.to_java_fixed_value())
        gateway.entry_point.saveAndClearFixedValueBuffer(fixed_list)

    def save_subject_types(self, subject_types):
        global gateway
        is_list_object(subject_types)
        all_same_type(SubjectType, subject_types)

        sub_type_list = gateway.jvm.java.util.ArrayList()
        for sub_types in subject_types:
            sub_type_list.append(sub_types.to_java_subject_type())
        gateway.entry_point.saveSubjectTypes(sub_type_list)

    def save_subjects(self, subjects):
        global gateway
        is_list_object(subjects)
        all_same_type(Subject, subjects)

        subject_list = gateway.jvm.java.util.ArrayList()
        for subject in subjects:
            subject_list.append(subject.to_java_subject())
        gateway.entry_point.saveAndClearSubjectBuffer(subject_list)


    def save(self, provider=None, attributes=None, subject_types=None, subjects=None, fixed_values=None, timed_values=None):
        global gateway
        self.start_server()

        params = [attributes, subject_types, subjects, provider, fixed_values, timed_values]
        func = {'Provider': self.save_provider, 'Attribute': self.save_attribute, 
                'SubjectType': self.save_subject_types, 'Subject': self.save_subjects, 
                'FixedValue': self.save_fixed_values, 'TimedValue': self.save_timed_values}

        database = gateway.jvm.uk.org.tombolo.core.utils.HibernateUtil()
        database.startUpForPython()
        for p in params:
            if p is not None:
                if isinstance(p, list):
                    v = p[0]
                    func[v.__class__.__name__](p)
                else:
                    func[p.__class__.__name__](p)

        database.shutdown()
        gateway.shutdown()


    """
    This is a test method and needs to be removed later
    """
    # def test(self):
        # self.download_data(url='http://api.erg.kcl.ac.uk/AirQuality/Annual/MonitoringObjective/GroupName=London/Year=2010/json', 
        #                     data_cache_directory=home_dir + '/Desktop', prefix='', suffix='.json')



class RunPy4jServer(threading.Thread):
    def __init__(self, tombolo_path):
        threading.Thread.__init__(self)
        self._tombolo_path = tombolo_path

    def run(self):
        global server_started
        if not server_started:
            jars_for_classpath = self.class_path_files()
            args = ['java', '-cp', 
            home_dir + self._tombolo_path + 'lib/py4j0.10.6.jar:' + home_dir + self._tombolo_path +  'build/classes/java/main:' + 
            ':'.join(jars_for_classpath), 'uk.org.tombolo.Py4jServer']
            p = sp.Popen(args, cwd=home_dir + self._tombolo_path)
        server_started = True
    
    def class_path_files(self):
        import os, os.path
        dirs = []
        for directory_path, _, file_names in os.walk(home_dir + "/.gradle/caches/modules-2/files-2.1"):
            for file_name in [f_names for f_names in file_names if f_names.endswith(".jar")]:
                dirs.append(os.path.join(directory_path, file_name))
        return dirs





"""
This is also a test section and needs to be removed later
"""
# tombolo_path = '/Desktop/UptodateProject/TomboloDigitalConnector/'
# importer = AbstractImporter(tombolo_path)
# # # importer.test()
# # # importer.save_provider(Provider(label='test label', name='test name'))
# geo = Geometry(latitude='51.524086', longitude='-0.104831')
# p = Provider(label='test3', name='test3')
# attr = Attribute(provider=p, label='attribute_test', description='this is a test')
# sub_type = SubjectType(provider=p, label='something', name='thisname')
# subject = Subject(subject_type=sub_type, label='sublable', name='subname', shape=geo.to_java_geometry())
# # importer.save(subjects=[subject], provider=p, subject_types=[sub_type], attributes=[attr])
# importer.save(subjects=[subject])
# importer.save(provider=Provider(label='test3', name='test3'))
# importer.save(attributes=[Attribute(provider=p, label='attribute_test', description='this is a test')])


