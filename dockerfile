FROM apache/airflow:2.5.1

# Atualizar os pacotes e instalar dependências necessárias antes do Java
USER root
RUN apt-get update && apt-get install -y curl gnupg && apt-get clean

# Instalar OpenJDK 11 (necessário para PySpark)
RUN apt-get update && apt-get install -y openjdk-11-jdk && apt-get clean

# Definir variável de ambiente do Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Voltar para o usuário airflow
USER airflow

# Instalar PySpark
RUN pip install pyspark
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark
RUN pip install --no-cache-dir apache-airflow-providers-microsoft-azure

# Baixar o JDBC Driver do SQL Server e mover para a pasta do Spark
RUN wget https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/9.2.1.jre8/mssql-jdbc-9.2.1.jre8.jar -P /opt/spark/jars/

# JARs necessarios
RUN wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.1/hadoop-azure-3.3.1.jar \
    https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar
