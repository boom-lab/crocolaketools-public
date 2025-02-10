## CrocoLakeTools

CrocoLakeTools is a Python package containing modules to interface with CrocoLake -- a database of oceanographic observations that is developed and maintained in the framework of the NSF-sponsored project CROCODILE (CESM Regional Ocean and Carbon cOnfigurator with Data assimilation and Embedding).

Its strength is to offer a uniform interface to a suite of oceanographic observations, which are all maintained in the same format (parquet), offering the user with the opportunity to retrieve and manipulate observations while remaining agnostic of their original data format, thus avoiding the extra-learning required to deal with multiple formats (netCDF, CSV, parquet, etc.) and merging different sources.

### Table of Contents
1. [Installation](#installation)
2. [Loading data from CrocoLake](#loading-data-from-crocolake)
   1. [Loading the data](#loading-the-data)
      1. [Filter variables](#filter-variables)
      2. [Filter sources](#filter-sources)
      3. [Filter by variables values](#filter-by-variables-values)
3. [Available sources](#available-sources)
4. [The `converter` submodule](#the-converter-submodule)

### Installation
On your terminal, run:
```
pip install crocolaketools
```

If you encounter some dependency issues, try running
```
pip install crocolaketools -c constraints.txt
```
##### Download CrocoLake
To download the most recent version of CrocoLake, save it from this link, or run:
```
wget [link] path/to/my/CrocoLake
```
If you don't specificy a path, CrocoLake is downloaded in the same directory where you run `wget`.

### Loading data from CrocoLake

What follows is a brief guide on how to load data from CrocoLake. More examples (including how to manipulate the data) are in the [`notebooks`](crocolaketools/notebooks/) folder.

Before going ahead, remember to [download](download-crocolake) CrocoLake if you haven't already.

The simplest way to load it into your working space is through the `Loader` class:
```
from crocolaketools.loader.loader import Loader
loader = Loader.Loader(
    db_type="PHY",  # specify "PHY" or "BGC" for physical or biogeochemical databases
    db_rootpath="/path/to/my/CrocoLake"
)
ddf = loader.get_dataframe()
```
`Loader()` needs at minimum the database type (`"PHY"` or `"BGC"`) and the path to the database. `get_dataframe()` returns a dask dataframe. If you're not familiar with dask, you can think of it as a wrapper to deal with data that are larger than what your machine's memory can load. A dask dataframe behaves almost identically like a pandas dataframe, and if you indeed want to use a pandas dataframe, you can just do (but DON'T do it, yet):
```
df = ddf.compute()
```
Note that this will load into memory all the data that `ddf` is referencing to: our first simple example would load more data than most systems can handle, so let's see how we can apply some filters.

##### Filter variables
If you want to load only some specific variables (see list [here](crocolaketools/utils/params.py)), you can pass a name list to `Loader()`:
```
selected_variables = [
    "LATITUDE",
    "LONGITUDE",
    "PRES",
    "PSAL",
    "TEMP"
]

loader = Loader.Loader(
    selected_variables=selected_variables,
    db_type="PHY",
    db_rootpath="/path/to/my/CrocoLake"
)

ddf = loader.get_dataframe()
```

##### Filter sources
Similarly, you can also filter by data source (list [here](crocolaketools/utils/params.py)) with a list:
```
db_source = ["ARGO"]

loader = Loader.Loader(
    selected_variables=selected_variables,
    db_type="PHY",
    db_list=db_source,
    db_rootpath="/path/to/my/CrocoLake"
)

ddf = loader.get_dataframe()
```

##### Filter by variables values
Filtering by values (i.e. row-wise, e.g. to restrain the geographical coordinates or time period) requires to define and apply a filter to the `loader` object:
```
filters = [
    ("LATITUDE",'>',5),
    ("LATITUDE",'<',30),
    ("LONGITUDE",'>',-90),
    ("LONGITUDE",'<',-30),
    ("TEMP",">=",-1e30),
    ("TEMP","<=",+1e30)
]

loader.set_filters(filters)

ddf = loader.get_dataframe()
```
Two notes on the filters:
   * To discard invalid values (NaNs), request the variable to be inside a very large interval (e.g. between `-1e30` and `+1e30`)
   * The filters must be passed in the appropriate format (see the filters option [here](https://docs.dask.org/en/stable/generated/dask.dataframe.read_parquet.html)); it's easier done than explained, but basically a single list contains AND predicates, and outer, parallel lists are combined with OR predicates. In the example above, all conditions must be satisfied by a row to be kept into the dataframe. If we want to keep all the rows with valid temperature or pressure values in the region, we would do:
```
filters = [
    [
        ("LATITUDE",'>',5),
        ("LATITUDE",'<',30),
        ("LONGITUDE",'>',-90),
        ("LONGITUDE",'<',-30),
        ("TEMP",">=",-1e30),
        ("TEMP","<=",+1e30)
    ],[
        ("LATITUDE",'>',5),
        ("LATITUDE",'<',30),
        ("LONGITUDE",'>',-90),
        ("LONGITUDE",'<',-30),
        ("PRES",">=",-1e30),
        ("PRES","<=",+1e30)
    ]
]

```
#### The `converter` submodule

The `converter` submodule contains several classes to convert from one source to the parquet format. It requires that the machine hosts the original sources (e.g. GLODAP's master file). This is considered an advanced use of CrocoLakeTools and at the moment we refer the interested user to the examples in the `scripts` folder to see how to set up a converter (or reach out!).

### Available sources

As of this release, CrocoLake includes all the [Argo](https://argo.ucsd.edu/) physical and biogeochemical data present in the GDAC, [GLODAP](https://glodap.info/)'s database, and QC-ed observations from [Spray Gliders](https://spraydata.ucsd.edu/about/spray-glider).

We are always working on including new sources, and the next candidates are the [North Atlantic CPR Survey](https://www.bco-dmo.org/project/547835) and the [Oleander project](https://www.aoml.noaa.gov/phod/goos/oleander/intro.php).

If you are interested in a particular dataset to be added, [get in touch](enrico.milanese@whoi.edu)!
