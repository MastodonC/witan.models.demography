# Introduction to witan.models.demography


`witan.models.demography` is a Clojure library to run demography models.

Ultimately it will be a tool to run:

* population projections
* school rolls projections
* small area projections


`witan.models.demography` proposes different ways to run each of those three types of projections.
The different methodologies depend on your preferences and/or the data available to you.

Note: Both school rolls projections and small area projections take as an input population projections.


**The content of the library follows this structure:**
* [Population projections](#population-projections)
  - [Trend-based Cohort Component Model](#trend-based-cohort-component-model)

	1) Fertility
	   * Historic fertility rates - births by age of the mother
	   * Historic fertility rates - total births
	   * Future fertility rates

	2) Mortality
	   * Historic mortality rates - deaths by age and by sex
	   * Historic mortality rates - total deaths
	   * Future mortality rates

	3) Migration
	   * Historic migration rates
	   * Future migration rates

	4) Forward projection
  - [Housing-led Cohort Component Model](#housing-led-cohort-component-model)
  - [Employment-led Cohort Component Model](#employment-led-cohort-component-model)

* [School rolls projections](#school-rolls-projections)

* [Small area projections](#small-area-projections)


## Population projections

We have chosen to implement the cohort component method as it is the most commonly used method for simple population projections.
More complex methods aren't currently supported in `witan.models.demography`.

<pre>Cohort component population projection:

This method begins with a base population, usually categorised by age and sex.
This base population evolves through the years by applying assumptions regarding mortality, fertility and migration.
This procedure gives, for every year of the projection, a distribution of the population by age and sex. [1]

The ideas behind this method can be represented as an equation:

P(t+n)= P(t) + B(t) − D(t) + I(t) − E(t)

where:
    P(t) is the population at time t
    B(t) and D(t) are number of births and deaths occurring between t and t+n.
    I(t) and E(t) are the number of immigrants and of emigrants from the country
    during the period t to t+n.

[2]
</pre>

Three alternatives are available to produce population projections:

### Trend-based Cohort Component Model
<pre>    |
    |--> Historic fertility rates          |--> Future fertility rates |
    |    |--> Historic births/mother's age |                           |
    |    |--> Total births                 |                           |
    |                                                                  |
    |--> Historic mortality rates          |--> Future mortality rates |--> Forward projection
    |    |--> Historic deaths/age/sex      |                           |
    |    |--> Total deaths                 |                           |
    |                                                                  |
    |--> Historic migration rates          |--> Future migration rates |
</pre>

This method involves projecting births, deaths and migrations by age and sex.

1) Fertility

The standard methodology for birth projections relies on applying age-specific fertility rates (ASFR)
to the female population and split the estimated births into male and female using a standard ratio.

* Historic data on births by mother's age


* Total births data


2) Mortality

* Historic data on deaths by age and by sex

* Total deaths data


3) Migration


### Housing-led Cohort Component Model


### Employment-led Cohort Component Model

<br>

## School rolls projections

There is one possibility to produce school rolls projections

<br>

## Small area projections

There are two alternatives to produce small area projections

### Area proportional distribution


### Small area projection model

___
1. S. Pennec, 2009, [APPSIM - Cohort Component Population Projections to Validate and Align the Dynamic Microsimulation Model APPSIM](http://www.natsem.canberra.edu.au/publications/?publication=appsim-cohort-component-population-projections-to-validate-and-align-the-dynamic-microsimulation-model-appsim)
2. United Nations Population Fund (UNFPA), Population Analysis for Policies & Programmes, [concepts and methods](http://papp.iussp.org/sessions/papp101_s10/PAPP101_s10_060_010.html)
