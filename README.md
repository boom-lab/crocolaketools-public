## CrocoLakeTools

CrocoLakeTools is a Python package containing modules to interface with CrocoLake -- a database of oceanographic observations that is developed and maintained in the framework of the NSF-sponsored project CROCODILE (CESM Regional Ocean and Carbon cOnfigurator with Data assimilation and Embedding).

It is composed of two submodules, `downloader` and `converter`, which retrieve the most recent version of some ocean observations datasets and convert them to CrocoLake's format. It share some utilities with [CrocoLakeLoader](https://github.com/boom-lab/crocolakeloader/tree/9758ba72f1d6fc968a5cfabf8740a49eef9022ea), the package to load and manipulate CrocoLake's data, and it is included as a submodule.

### Table of Contents
1. [Installation](#installation)
2. [converter](#converter)
3. [downloader](#downloader)
4. [Available sources](#available-sources)

### Installation
Clone the repository locally and, on your terminal, run:
```
pip install ./crocolakeloader/
pip install .
```

If you encounter some dependency issues, try running
```
pip install ./crocolakeloader/
pip install . -c constraints.txt
```

### `converter`

The `converter` module contains several classes to convert from one source to the parquet format; they all inherit from the `Conveter()` class and follow the same workflow, but each step can be highly specialized depending on the observations source. For GLODAP and Spray Gliders data, it requires that the machine hosts the original sources. The examples in the `scripts` folder to show how to set up and run an existing converter.

### `downloader`

The `downloader` module downloads the observations sources to convert. It currently contains tools to download Argo's GDAC only.

### Available sources

As of this release, CrocoLake includes all the [Argo](https://argo.ucsd.edu/) physical and biogeochemical data present in the GDAC, [GLODAP](https://glodap.info/)'s database, and QC-ed observations from [Spray Gliders](https://spraydata.ucsd.edu/about/spray-glider).

We are always working on including new sources, and the next candidates are the [North Atlantic CPR Survey](https://www.bco-dmo.org/project/547835) and the [Oleander project](https://www.aoml.noaa.gov/phod/goos/oleander/intro.php).

If you are interested in a particular dataset to be added, [get in touch](enrico.milanese@whoi.edu)!
