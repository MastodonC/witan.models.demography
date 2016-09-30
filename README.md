# witan.models.demography

[![Build Status](https://circleci.com/gh/MastodonC/witan.models.demography.svg?style=shield)](https://circleci.com/gh/MastodonC/witan.models.demography) [![Dependencies Status](https://jarkeeper.com/MastodonC/witan.models.demography/status.svg)](https://jarkeeper.com/MastodonC/witan.models.demography)

## Table of content:

* [Description](#description)

* [Contribute to `witan.models.demography`](#contribute-to-witanmodelsdemography)
  * [General practices](#general-practices)
  * [Development tools](#development-tools)

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
