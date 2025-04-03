# Big Data Project

## Overview
Welcome to the Big Data Project repository! This project focuses on processing and analyzing large datasets using technologies such as Hadoop, MapReduce, and other big data frameworks. It includes various modules for data preprocessing, merging, and final analysis.

## Table of Contents
- [Overview](#overview)
- [Project Structure](#project-structure)

## Project Structure

```bash
.
├── Archive_Notebooks_Testing/
│   ├── eda.ipynb
│   ├── JJ_Showcase.ipynb
│   ├── plotly.ipynb
│   └── WeightedAverageAndVariance.ipynb
├── DataAnalysis/
│   ├── Rating_Analysis.ipynb
│   ├── Review_Analysis.ipynb
│   └── SentimentAnalysis.ipynb
├── EDA/
    ├── spark_eda.ipynb
├── src/
│   └── main/
│       └── java/
│           ├── analysis/              # Analysis-related Java code
│           ├── archive/               # Old or archived code (for reference)
│           ├── final_merge/           # Final merging stage
│           │   ├── FinalJoinDriver.java
│           │   ├── FinalJoinMapper.java
│           │   ├── FinalMetaJoinMapper.java
│           │   └── FinalReviewReducer.java 
│           ├── merge/                 # Intermediate merging stage
│           │   ├── JoinReducer.java
│           │   ├── MergeReviewsAndMetaDriver.java
│           │   └── ReviewsJoinMapper.java
│           └── preprocessing/         # Data preprocessing components
│               ├── CleanReviewsDriver.java
│               ├── CleanReviewsMapper.java 
│               └── CleanReviewsReducer.java 
├── docs/                
└── README.md             # This file
