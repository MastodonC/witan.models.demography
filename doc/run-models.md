# How to produce population projections

## Introduction
`witan.models.demography` is a Clojure library to run demographic models.

The aim of `witan.models.demography` is to offer "generic" models in the sense that our models will be adaptable to different methodologies and data inputs. To learn more about the methodology, see the [modelling docs](intro.md). To learn more about the data inputs currently being used, jump to the [data inputs](#data-inputs) section.

A minimal version of the population projection is now available for English unitary authorities, non-metropolitan districts, and boroughs. Projections are also available for the countries of Northern Ireland, Scotland, and Wales. 

## Running a population projection

An executable.jar file, which contains all the code and data needed to produce a projection, can be downloaded from http://example.com/FIXME.

The .jar file contains the following:

![minimal CCM](images/jar-setup.png)

To run the .jar file and produce a projection, follow the instructions in the [README](repo/blob/master/README.md).

## User-defined parameters for the model

Some parameters in the model need to be specified by the user. The .jar file contains default values for the parameters, which can be changed as needed. The parameters, with defaults in parentheses, are:

* First year of projection (2015)
* Last year of projection (2040)
* Last year of historic fertility data (2014)
* Start & end years of year range to average over when projecting deaths (2010-2014)
* Start & end years of year range to average over when projecting domestic migration (2010-2014)
* Start & end years of year range to average over when projecting international migration (2010-2014)
* Proportion of male newbors (0.5122)

## User-defined parameters for the geography

The geographical area for the projection must be specified. This is done with the 9-digit GSS code (Government Statistical Service code), or name, of one of the following:

  * an English unitary authority (starts with "E06")
  * an English non-metropolitan district (starts with "E07")
  * an English metropolitan borough (starts with "E08")
  * a London borough (starts with "E09")
  * Northern Ireland, Scotland, or Wales (country codes "N92000002", "S92000003", or "W92000004"; smaller geographis not yet available)
  

## Data inputs

The historical data required for the projection is included in the .jar file and comes from the UK's Office of National Statistics. The datasets used are:

* Historic population
* Projections of births by age of mother
* Historic births
* Historic deaths
* Domestic in-migrants
* Domestic out-migrants
* International in-migrants
* International out-migrants

### Columns expected in each data input

Some datasets have been reformatted from their original structure. The column names and column order required for each dataset is given below, as well as a first row of example data. 

#### Historic Population

| GSS Code  | Sex | Age | Year | Births |
| --------- | --- | --- | ---- | ------ |
| E06000023 |  F  |  0  | 2001 | 2330 |

#### Projections of births by age of mother


| GSS Code  | Sex | Age | Year | Births |
| --------- | --- | --- | ---- | ------ |
| E06000023 |  F  | 15  | 2013 | 7.768  |

#### Historic births

| GSS Code  | Sex | Age | Year | Births |
| --------- | --- | --- | ---- | ------ |
| E06000023 |  F  | 0  | 2002 | 2320  |

#### Historic deaths

| GSS Code  | Sex | Age | Year | Deaths |
| --------- | --- | --- | ---- | ------ |
| E06000023 |  F  | 0  | 2002 | 9  |

#### Domestic in-migrants

| GSS Code  | Sex | Age | Var | Year | Estimate |
| --------- | --- | --- | ---- | ------ | ----- |
| E06000023 |  F  | 0  | internal-in | 2002 | 70 |

#### Domestic out-migrants

| GSS Code  | Sex | Age | Var | Year | Estimate |
| --------- | --- | --- | ---- | ------ | ----- |
| E06000023 |  F  | 0  | internal-out | 2002 | 150 |

#### International in-migrants

| GSS Code  | Sex | Age | Var | Year | Estimate |
| --------- | --- | --- | ---- | ------ | ----- |
| E06000023 |  F  | 0  | international-in | 2002 | 16 |

#### International out-migrants

| GSS Code  | Sex | Age | Var | Year | Estimate |
| --------- | --- | --- | ---- | ------ | ----- |
| E06000023 |  F  | 0  | international-out | 2002 | 4 |