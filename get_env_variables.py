import os

os.environ['envn'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'
os.environ['SPARK_HOME'] = 'C:\Tools\spark-3.5.0-bin-hadoop3-scala2.13'
os.environ["JAVA_HOME"] = 'C:\Program Files\Java\jdk-19'


# os.environ["JAVA_HOME"] = '/usr/lib/jvm/java-9-openjdk-amd64'

header = os.environ['header']

inferSchema = os.environ['inferSchema']
envn = os.environ['envn']

appName = 'pysparkPro'

current = os.getcwd()

src_olap = current + '\source\olap'
src_oltp = current + '\source\oltp'
src_unstruc = current + '\source\\unstructured_data'

ps_driver = current + '\jars\postgresql-42.7.2.jar'

