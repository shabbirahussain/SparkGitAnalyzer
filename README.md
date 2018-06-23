# SparkGitAnalyzer

This project is meant to provide tools to analyze the data collected by SparkGitDownloader.

# Usage
## Pre-Requisites
1. Maven commandline (mvn command should work in your shell)
1. Apache Spark Shell. You can download it from their website and put its bin folder in your path.
1. After building just put in path of raw data collected from the crawler (outermost in the `config-defaults.properties` file found in `target/bundle/resources/conf`)

## Compiling
```
make build
```

## Executing
```
make run
```


# Basics of this tool

1. It will create a free to analyze `spark-shell`. (Similar to scala shell with spark functionalities in it).
1. It will also load some basic queries from `org/reactorlabs/jshealth/analysis/queries/__init__.scala` these will serve as data entry point.
1. Finally user can execute their own queries on data. Some more sample queires are shown in the `org.reactorlabs.jshealth.analysis.queries` package.

##### TIP:
We have baked in functionality of sticky checkpoint. Which means whenever you call checkpoint on DataFrame with an string argument specifying name, the code will checkpoint the data for the first time. On every sucessive execution and I mean even after you restarted your machine. It will pull in data from that savepoint/checkpoint. To clean this checkpoint kindly remove the directory with that name from the path configured in the `config-defaults.properties`.