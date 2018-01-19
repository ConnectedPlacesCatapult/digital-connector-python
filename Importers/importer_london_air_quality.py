from os import path, pardir
import sys
sys.path.append(path.join(path.dirname(path.realpath(__file__)), pardir))

from Importer import Provider, SubjectType, AbstractImporter, home_dir
import convert_to_dataframe
import pandas as pd
import json


data_url = 'http://api.erg.kcl.ac.uk/AirQuality/Annual/MonitoringObjective/GroupName=London/Year=2010/json'
importer = AbstractImporter(tombolo_path='/Desktop/UptodateProject/TomboloDigitalConnector/')

provider = Provider(label='erg.kcl.ac.uk', name='Environmental Research Group Kings College London')
subject_type = SubjectType(provider=provider, label='erg.kcl.ac.uk', name='airQualityControl')
importer.download_data(url=data_url, data_cache_directory=home_dir + '/Desktop', prefix='', suffix='.json')
fetch_data = importer._data
to_dataframe = convert_to_dataframe.convert(data=fetch_data)
data = pd.DataFrame(data=to_dataframe)


print(data.head())

