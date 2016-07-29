# witan.models.demography

[![Build Status](https://circleci.com/gh/MastodonC/witan.models.demography.svg?style=shield)](https://circleci.com/gh/MastodonC/witan.models.demography) [![Dependencies Status](https://jarkeeper.com/MastodonC/witan.models.demography/status.svg)](https://jarkeeper.com/MastodonC/witan.models.demography)


## Description

`witan.models.demography` is a Clojure library to run demographic models.

Those models will be used on MastodonC's [Witan](http://www.mastodonc.com/products/witan/) city decision-making platform.
They can also be used independently of Witan as a standalone demographic models library.

Current status:
* Population projections: First release of a minimal version coming soon!

See the [docs](https://github.com/MastodonC/witan.models.demography/blob/master/doc/intro.md) for more info about the methodology.

## Development

The `:dev` profile has a dependency for `witan.workspace-executor` ont top of `witan.workspace-api` which means the two following tools can be used.

* To visualise a model workflow, use the `view-workflow` function:
```Clojure
(witan.workspace-executor.core/view-workflow
   (:workflow witan.models.dem.ccm.models/cohort-component-model))
```

* To print logs whenever a `defworkflowfn` is called (useful for debugging purpose), use the
`set-api-logging!` function:
```Clojure
(witan.workspace-api/set-api-logging! println)
```

Turn it off with:
```Clojure
(witan.workspace-api/set-api-logging! identity)
```

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

	$ lein with-profile cli uberjar
```

Copy the filepath to the standalone file to use later.

## Usage
Population projections can be produced by running the executable .jar file from the command line:

```
    $ java -jar [path/to/jar] -c ["GSS code"] -i [optional/path/to/input/config/file] -o [optional/path/to/output/csv]
```

For example:
```
    $ java -jar witan.models.demography/target/uberjar/witan.models.demography-0.1.0-SNAPSHOT-standalone.jar -c "E06000023" -i "default_config.edn" -o "ccm_projections.csv"
```
A GSS code must always be specified to tell the model which geographic area the projections are for. See the GSS codes that are accepted [here](#gss-code).

## Options

The projections can be customised by adjusting the options below. Read more about how the .jar file works and what input data is expected in the [docs](doc/run-models.md).

* [`-c` or `--gss-code`](#gss-code)
* [`-i` or `--input-config`](#input-config)
* [`-o` or `--output-projections`](#output-projections)


The last two options have default values if not specified:

* default input configuration filepath: "default_config.edn"
* default output projection filepath: "ccm_projections.csv"

These default settings mean the "default_config.edn" file is located inside this repository and the output will be created as "ccm_projections.csv" inside this repository as well.

The previous command can thus been run as follows:

```
	$ java -jar witan.models.demography/target/uberjar/witan.models.demography-0.1.0-SNAPSHOT-standalone.jar -c "E06000023"
```

Example:

To save the projections output to a different directory, with a different filename:

```
	$ java -jar witan.models.demography/target/uberjar/witan.models.demography-0.1.0-SNAPSHOT-standalone.jar -c "E06000023" -o /home/user/Documents/ccm_projections/Bristol_projections.csv"
```

### gss-code

The geographical area for the projection must be specified. This is done with the 9-digit GSS code (Government Statistical Service code), or name, of one of the following:

  * an English unitary authority (starts with "E06")
  * an English non-metropolitan district (starts with "E07")
  * an English metropolitan borough (starts with "E08")
  * a London borough (starts with "E09")
  * Northern Ireland, Scotland, or Wales (country codes "N92000002", "S92000003", or "W92000004"; smaller geographies not yet available)

### input-config

The configuration file contains file paths to the model input datasets and model parameters that can be adjusted by the user. The parameters in this file are described [here](doc/run-models.md).

The [default config file](https://github.com/MastodonC/witan.models.demography/blob/master/default_config.edn) should be used as a template and edited at its current location or copied (and modified) anywhere else on your machine.

### output-projections

This is the path to the file where the final projection should be saved. The projections are returned in one csv file. This file contains the historical population data used in the projection, with the projected population appended. The output path can be anywhere on your machine as long as the directories on the path exist.


## License

Copyright © 2016 MastodonC Ltd

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
