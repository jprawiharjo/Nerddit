# Nerddit

This project was done in insight in the course of ~2-3 weeks.

## Motivation
Reddit is a social media network which is rich in cultures.

My motivations are twofolds:

1. N-grams viewer allows data scientist to investigate for topics trends, language analysis, etc. that happens in Reddit as a function of time
2. I'm interested in subreddits graph, how they are connected to each others by means of common users.

## Methods

The technologies used in this project are listed below:
- Amazon S3
- Spark
- Cassandra
- Flask

The pipeline for the data flow are shown in the figure below:

![Pipeline](/images/pipeline.png?raw=true "Pipeline")

Reddit comments data from October 2007 to December 2015 were downloaded from the internet, and stored in Amazon S3. The data was >1TB in total, in JSON format.

I used Spark to perform ETL from S3 to Cassandra in 4 tables.

## Graph visualization

The graph from Cassandra were processed in Python-NetworkX to filter "over-subscribed" subreddits, which I defined as nodes with degree >100. This choice was taken to reveal clustering of the subreddits. Gephi was used to find the clusterings of the subreddits, and applied Force Atlas 2 layout algorithm and then visualized by Sigma.js.
