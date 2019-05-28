[![Build Status](https://travis-ci.org/HSLdevcom/transitdata-hfp-downgrader.svg?branch=master)](https://travis-ci.org/HSLdevcom/transitdata-hfp-downgrader)

## Description

Application for reading HFP v2 data from MQTT broker and feeding it back into MQTT broker as HFP v1 data.

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

Either use released versions from public maven repository or build your own and install to local maven repository:
  - ```cd transitdata-common && mvn install```  

### Locally

- ```mvn compile```  
- ```mvn package```  

### Docker image

- Run [this script](build-image.sh) to build the Docker image

## Running

Requirements:
- Connection to an external MQTT server.
  - Configure username and password via files
    - Set filepath for username via env variable FILEPATH_USERNAME_SECRET, default is `/run/secrets/mqtt_broker_username`
    - Set filepath for password via env variable FILEPATH_PASSWORD_SECRET, default is `/run/secrets/mqtt_broker_password`
  - Mandatory: Set HFP v1 MQTT topic via env variable MQTT_TOPIC
  - Remember to use a unique MQTT client-id's if you have multiple instances connected to a single broker.

All other configuration options are configured in the [config file](src/main/resources/environment.conf)
which can also be configured externally via env variable CONFIG_PATH

Launch Docker container with

```docker-compose -f compose-config-file.yml up <service-name>```   
