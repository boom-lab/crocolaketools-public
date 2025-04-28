---
title: 'CrocoLakeTools: A Python package to convert ocean observations to the parquet format'
tags:
  - Python
  - oceanography
  - measurements
  - datasets
  - parquet
  - Argo
authors:
  - firstname: Enrico
    surname: Milanese
    orcid: 0000-0002-7316-9718
    correponding: true
    affiliation: 1
  - firstname: David
    surname: Nicholson
    orcid: 0000-0003-2653-9349
    affiliation: 1
  - firstname: Gaël
    surname: Forget
    affiliation: 2
  - firstname: Susan
    surname: Wijffels
    affiliation: 1
affiliations:
 - name: Woods Hole Oceanographic Institution, United States of America
   index: 1
 - name: Massachusetts Institute of Technology, United States of America
   index: 2
date: 14 April 2025
bibliography: paper.bib

---

# Summary
Investigations of the ocean state are possible thanks to the ever growing number
of measurements performed with multiple instruments by different research
missions. The vast and variegated efforts have brought the community to define
data storage conventions (e.g. CF-netCDF) and to assemble collections of
datasets (e.g. the World Ocean Database). Yet, accessing these datasets often
requires the usage of multiple tools, is inefficient over the cloud, and
presents an overall high entrance barrier in terms of knowledge and time
required to effectively access these resources. CrocoLakeTools is a Python
package that address those shortcomings by providing workflows to convert
several datasets from their original format to a uniform parquet dataset with a
shared schema.

# Statement of need
`CrocoLakeTools` is a Python package to build workflows that convert ocean
observations from different formats (e.g. netCDF, CSV) to parquet.
CrocoLakeTools take advantage of Python's well-established and growing ecosystem
of open tools: it uses `dask`'s parallel computing capabilities to convert
multiple files at once and to handle larger-than-memory data. `dask` is already
well-integrated with `xarray` and `pandas`, two widely used Python libraries for
the treatment of array and tabular data, respectively, and with `pyarrow`, the
API to the Apache Arrow library which is used to generate the parquet dataset.

Parquet is a data storage format for big tidy data which presents several
advantages: it is language agnostic (it can be accessed with Python, Matlab,
Julia and web developement technologies); it offers faster reading performances
than other tabular formats such as CSV; it is optimized for cloud systems
storage and operations; it is widespread in the data science community, leading
to a multitude of freely accessible tools and educational material.

`CrocoLakeTools` was developed with the goal of building and serving CrocoLake,
a regularly refreshed database of oceanographic observations that are
pre-filtered to contain only quality-controlled measurements. `CrocoLakeTools`
was designed to be used by researchers and data scientists and engineers in
oceanograhy. `CrocoLake` was designed to be accessed by the wider oceanographic
community.

# Code architecture

## Converters

The core task of CrocoLakeTools is to take one or more files from a dataset and convert them to parquet, ensuring that CrocoLake's schema is followed. This is achieved through the methods contained in the `Converter` class and its subclasses. While the conversion of all datasets requires some general functionality (e.g. renaming the original variables to the final schema), each conversion requires specific tools for the specific dataset (e.g. the map used to rename the variable). CrocoLakeTools then hosts a converter for each dataset that implement the specific needs of that datasets and inherits from `Converter`, which contains the shared methods.

## Workflow
The first step in the workflow is to retrieve the original files. This is generally left to the user, although we provide methods to download Argo data and we wish to be able in the future to provide this type of tools for other datasets as well. 

### Sources
These are the original data provided by the project, mission, scientist, etc.
The format, schema, nomenclature and conventions are those defined by the individual project and are unaware of CrocoLake's workflow.

### Storage (original)
Modules to download the original data are optional. They should inherit from the `Downloader` class and be called `downloader<ProjectName>`, e.g. `downloaderArgoGDAC`. Whether a downloader module exists or the user downloads the data themselves, the original data is stored on disk and this is the starting point for the converter. 

### Storage (converted)
The core of `CrocoLakeTools` are the modules in the `Converter` class and its subclasses. Each project has its own subclass called `converter<ProjectName>`, e.g. `converterGLODAP`; further specifiers can be added as necessary (e.g. at this time there a few different converters for Argo data to prepare different datasets). The need for a dedicated converter for each project despite the usage of common data formats (e.g. netCDF, CSV) is due to differences in the schema, e.g. variable names, units, etc., while the steps that can be generalized for each format are usually already included in other libraries that `CrocoLakeTools` rely on (e.g. `pandas`, `dask`, `pyarrow`).
Depending on the dataset, multiple converters can be applied. For example, to create CrocoLake, Argo data goes through two converters:
1. `converterArgoGDAC`, which converts the original Argo GDAC preserving most of its original conventions;
2. `converterArgoQC`, which takes the output of the previous step and applies some filtering based on Argo's QC flags and makes the data conforming to CrocoLake's schema.

### CrocoLake
CrocoLake contains each converted dataset. The first step to build it is to create a directory containing symbolic links to the converted datasets (an example script is provided). The submodule `CrocoLakeLoader` then allows to seamlessly load all the converted datasets into memory as one dask dataframe with a uniform schema, using just a few lines. The script `merge_crocolake.py` exploits `CrocoLakeLoader`'s capabilities to generate one merged CrocoLake dataset that contains all the converted datasets and is stored back to disk (in parquet). 


## Accessing CrocoLake
The `examples` folder contains examples for how to access parquet datasets with several programming languages: Python, Matlab, Julia. We hope to include soon R too. Separate open repositories contains more examples for each language, see: CrocoLake-Python, CrocoLake-Matlab, CrocoLake-Julia.

### Example
[TD ADD FIGURES]

# Figures

I am also trying this, maybe it works better with Fig \autoref{fig:workflow}.
\begin{figure}[h!]
    \centering
    \includegraphics[width=\textwidth]{workflow_01.png}
    \caption{Workflow.\label{fig:workflow}}
\end{figure}


## Documentation
Documentation is available at [TD add link]. It describes the CrocoLake dataset, thesub-datasets that it is made, and how they are obtained. It provides references to the original files origins for download before converting them.

## Citation
If you use CrocoLakeTools and/or CrocoLake, do not limit yourself to citing this manuscript. Remember to cite the datasets that you have used as indicated in the documentation. For example, if your work relies on Argo measurements, acknowledge Argo [@wong2020argo]. This is important for each product to track their impact. 



# Acknowledgements

We acknowledge funding from [NSF CSSI CROCODILE details]

# References

