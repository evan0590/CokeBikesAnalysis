# CokeBikesAnalysis

The bike share schemes, sponsored by Coca-Cola and operated by An Rothar Nua on behalf of the National Transport Authority, in Cork, Limerick and Galway have been in operation since 2015. It is fair to assume that the current health emergency has impacted the usage of these schemes. Since October 2020, usage data of these schemes has been gathered alongside weather information with a view toward analysing not only general usage patterns of the bike share schemes of these three cities but also analysing the relationship between usage patterns of the schemes and weather and time series data- should one exist. Python and its many packages for data analysis are used throughout this project.

This repository is separated into two main directories; namely ```database``` and ```analysis```. Contained within the ```database``` directory is code for creating and populating tables within a PostgreSQL database with the necessary information for analysis; whilst the ```analysis``` directory contains both Python scripts and jupyter notebooks that develop on the tasks set out below.

### Task 1: Data Preparation
In its raw format, the bike data consists of just six attributes related to bike and station availability, station identifiers, time data, whether a station is currently in use and so on. Two key attributes that need to be generated will be referred to from here on as ```count_1``` and ```count_2```. In order to generate these attributes and to properly merge the bike data with the weather data later on, it is going to be necessary to generate some additional support attributes.<br>
&ensp;1. Remove any rows related to stations that are not currently in use.<br>
&ensp;2. Generate the support attribute ```dd-mm-yy```.<br>
&ensp;3. Generate a city indicator attribute: 2 - Cork, 3 - Limerick, 4 - Galway.<br>
&ensp;4. Generate a bikes available citywide attribute that represents the total number of bikes available within each city for all recorded moments.<br>
&ensp;5. Generate a count attribute named ```count_1``` that cumulatively tracks bike usage at each station across every day.
&ensp;6. Generate a count attribute named ```count_2``` that displays the total bike usage at each city across every day.

*more to come soon*