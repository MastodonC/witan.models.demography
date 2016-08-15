# witan.models.demography

[![Build Status](https://circleci.com/gh/MastodonC/witan.models.demography.svg?style=shield)](https://circleci.com/gh/MastodonC/witan.models.demography) [![Dependencies Status](https://jarkeeper.com/MastodonC/witan.models.demography/status.svg)](https://jarkeeper.com/MastodonC/witan.models.demography)

## Table of content:

* [Description](#description)

* [Contribute to `witan.models.demography`](#contribute-to-witanmodelsdemography)
  * [General practices](#general-practices)
  * [Development tools](#development-tools)

* [Use `witan.models.demography`](#use-witanmodelsdemography)
  * [Requirements](#requirements)
  * [Installation](#installation)
  * [Usage](#usage)
  * [Options](#options)

* [License](#license)

## Description

`witan.models.demography` is a Clojure library to run demographic models.

Those models will be used on MastodonC's [Witan](http://www.mastodonc.com/products/witan/) city decision-making platform.
They can also be used independently of Witan as a standalone demographic models library.

Current status:
* Population projections: First release of a minimal version coming soon!

See the [docs](https://github.com/MastodonC/witan.models.demography/blob/master/doc/intro.md) for more info about the methodology.


## Contribute to `witan.models.demography`

### General practices

If you wish to contribute to `witan.models.demography`, please read the following guidelines.

#### Github guidelines
* **Fork this repository** (or clone it if you have writing rights to the reposotory).
* **Create a new branch**. Let's try and keep the following naming conventions for branches:
  * `feature/<what-this-branch-solves>`, example: `feature/project-mortality-rates`
  * `doc/<what-this-branch-solves>`, example: `doc/contributors-practices`
  * `fix/<what-this-branch-solves>`, example: `fix/run-ccm-from-cli`
  * `tidy-up/<what-this-branch-solves>`, example: `tidy-up/upgrade-deps`

  This way, when you see a branch starting by `fix/` we know something is broken and someone is repairing it.
* **Keep branches short** so that the reviewing process is easier and faster
* Start a pull request (PR) as early as possible. You can add a `WIP` in the title to specify it's in progress.
* Describe the aim of your changes in the PR description box.
* Before asking for a review of your PR:
  * **run all the tests** from the commmand line with `$ lein test`
  * **run the lint tools** [`Eastwood`](https://github.com/jonase/eastwood) and [`Kibit`](https://github.com/jonase/kibit) with `$ lein eastwood` and `$ lein kibit`.
  * **format your code** with [`Cljfmt`](https://github.com/weavejester/cljfmt) tool with `lein cljfmt check` followed by `lein cljfmt fix`.
  * **check the cohort-component model can still be run** from the command-line.
  You'll need to build the jar file, run it on your terminal and check it outputs population projections.
  For that follow the instructions in the ["user section"](#use-witanmodelsdemography) of the Readme.

#### Coding guidelines

* Write unit tests, docstrings, and documentation.
* Try to not have data changes and code changes in the same commit, and preferably not the same branch, as the data tends to swamp the code and hinder reviewing.
* Avoid modifying a file that is being modified on another branch.
* Avoid changing the name of a file while someone is working on another branch.
* We moved away from using Incanter library.
  To manipulate `core.matrix` datasets look for functions:
  * from `core.matrix` in [`core.matrix.datasets`](https://github.com/mikera/core.matrix/blob/develop/src/main/clojure/clojure/core/matrix/dataset.clj)
  * from the `witan.workspace-api` dependency in [`witan.datasets`](https://github.com/MastodonC/witan.workspace-api/blob/master/src/witan/datasets.clj)
* Have a look at the following paragraph for useful development tools.
* Commit and push your changes often using descriptive commit messages. And squash commits (as much as possible/necessary) before asking for a review.

### Development tools

When combining functions into a model there are useful tools to take advantage of, thanks to dependencies for `witan.workspace-executor` and `witan.workspace-api`.

#### To visualise a model workflow, you need to:

1) Install `Graphviz`:

- Ubuntu: `$ sudo apt-get install graphviz`

- MacOS: `$ brew install graphviz`

For any OS you should also be able to install it with Pip: `$ pip install graphviz`.

2) Use the `view-workflow` function using the [cohort-component-model](https://github.com/MastodonC/witan.models.demography/blob/master/src/witan/models/dem/ccm/models.clj) workflow as follows:

```Clojure
(witan.workspace-executor.core/view-workflow
   (:workflow witan.models.dem.ccm.models/cohort-component-model))
```

#### To print logs, use the `set-api-logging!` function:
```Clojure
(witan.workspace-api/set-api-logging! println)
```
Whenever a `defworkflowfn` is called logs will be written to your repl or terminal. It's very  useful for debugging purpose.

Turn it off with:
```Clojure
(witan.workspace-api/set-api-logging! identity)
```

## Use `witan.models.demography`

You can currently run a `witan.models.demography` cohort-component model on your terminal.
To test/use it, follow the instructions below.

### Requirements

You need to have [Leiningen](http://leiningen.org/) installed to run the projections.

### Installation

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

### Usage
Population projections can be produced by running the executable .jar file from the command line:

```
    $ java -jar [path/to/jar] -c ["GSS code"] -i [optional/path/to/input/config/file] -o [optional/path/to/output/csv]
```

For example:
```
    $ java -jar witan.models.demography/target/uberjar/witan.models.demography-0.1.0-SNAPSHOT-standalone.jar -c "E06000023" -i "default_config.edn" -o "ccm_projections.csv"
```
A GSS code must always be specified to tell the model which geographic area the projections are for. See the GSS codes that are accepted [here](#gss-code).

### Options

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

#### gss-code

The geographical area for the projection must be specified. This is done with the 9-digit GSS code (Government Statistical Service code), or name, of one of the following:

  * an English unitary authority (starts with "E06")
  * an English non-metropolitan district (starts with "E07")
  * an English metropolitan borough (starts with "E08")
  * a London borough (starts with "E09")

#### input-config

The configuration file contains file paths to the model input datasets and model parameters that can be adjusted by the user. The parameters in this file are described [here](doc/run-models.md).

The [default config file](https://github.com/MastodonC/witan.models.demography/blob/master/default_config.edn) should be used as a template and edited at its current location or copied (and modified) anywhere else on your machine.

#### output-projections

This is the path to the file where the final projection should be saved. The projections are returned in one csv file. This file contains the historical population data used in the projection, with the projected population appended. The output path can be anywhere on your machine as long as the directories on the path exist.

## Splitting and Uploading data

By default, the data for the CCM is amalgamated into single data files.
To split the files by GSS code, use the following command:

```
lein split-data
```

To upload all the CSV files to S3 (gzipped), use the following command:

```
lein upload-data
```

This assumes a valid AWS profile, called 'witan', is installed. For ease, these commands can be chained like so:

```
lein do split-data, upload-data
```

## License

Copyright © 2016 MastodonC Ltd

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
