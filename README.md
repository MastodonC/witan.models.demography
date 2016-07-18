# witan.models.demography

[![Build Status](https://circleci.com/gh/MastodonC/witan.models.demography.svg?style=shield)](https://circleci.com/gh/MastodonC/witan.models.demography) [![Dependencies Status](https://jarkeeper.com/MastodonC/witan.models.demography/status.svg)](https://jarkeeper.com/MastodonC/witan.models.demography)


## Description

`witan.models.demography` is a Clojure library to run demographic models.

Those models will be used on MastodonC's [Witan](http://www.mastodonc.com/products/witan/) city decision-making platform.
They can also be used independently of Witan as a standalone demographic models library.

Current status:
* Population projections: First release of a minimal version coming soon!

See the [docs](doc/intro.md) for more info about the methodology.

## Requirements

You need to have [Leiningen](http://leiningen.org/) installed to run the projections.

## Installation

1) Download or clone this repository.

2) Use the terminal to navigate to the directory that the repo is in, for example:
```
	$ cd witan.models.demography
```

3) Create an executable .jar file:
```
	$ lein clean

	$ lein uberjar
```

Copy the filepath to the standalone file, starting at the `target` directory, to use later. For example:
```
    target/uberjar/witan.models.demography-0.1.0-SNAPSHOT-standalone.jar
```

## Usage
Population projections can be produced by running the executable .jar file from the command line:  

```
    $ java -jar [path/to/jar] -c ["GSS code"] -i [optional/path/to/input/config/file] -o [optional/path/to/output/csv] 
```

For example:
```
    $ java -jar target/uberjar/witan.models.demography-0.1.0-SNAPSHOT-standalone.jar -c "E06000023" -i "default_config.edn" -o "ccm_projections.csv" 
```
A GSS code must always be specified to tell the model which geographic area the proejctions are for. See the GSS codes that are accepted [here](#gss-code).

## Options

The projections can be customised by adjusting the options below. Read more about how the .jar file works and what input data is expected in the [docs](doc/run-model.md).

* [`-c` or `--gss-code`](#gss-code)
* [`-i` or `--input-config`](#input-config)
* [`-o` or `--output-projections`](#output-projections)


The last two options have default values if not specified:

* default input configuration filepath: "default_config.edn"
* default output projection filepath: "ccm_projections.csv"

The previous command can thus been run as follows:

```
	$ java -jar target/uberjar/witan.models.demography-0.1.0-SNAPSHOT-standalone.jar -c "E06000023"
```

Example:

To save the projections output to a different directory, with a different filename:

```
	$ java -jar target/uberjar/witan.models.demography-0.1.0-SNAPSHOT-standalone.jar -c "E06000023" -o /home/user/Documents/ccm_projections/Bristol_projections.csv" 
```

### gss-code

The geographical area for the projection must be specified. This is done with the 9-digit GSS code (Government Statistical Service code), or name, of one of the following:

  * an English unitary authority (starts with "E06")   
  * an English non-metropolitan district (starts with "E07")   
  * an English metropolitan borough (starts with "E08")   
  * a London borough (starts with "E09")   
  * Northern Ireland, Scotland, or Wales (country codes "N92000002", "S92000003", or "W92000004"; smaller geographis not yet available)

### input-config

Configuration file with model parameters that can be adjusted by the user. The parameters in this file are described [here](doc/run-model.md).

See example: [`default_config.edn`](https://github.com/MastodonC/witan.models.demography/blob/master/default_config.edn)


### output-projections

Path to the file where the final projection should be saved. The projections are output as .csv files.


## License

Copyright © 2016 MastodonC Ltd

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.