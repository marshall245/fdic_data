# FDIC Project
Build and use a mongodb document store to build and maintain relational data in a NoSQL document format.

## Requirements
The following items are required to launch the project :
* Software : Docker, Git
* OS : Windows 10, Mac OS, Ubuntu

## About
Launching this project will create two Docker containers, one serving a MongoDB instance and the other serving a Jupyter notebook instance. Once the project has successfully launched, access to the Jupyter notebook will be available at localhost:5000. The database is then accessible through the Jupyter notebook at hostname mongodb-app and port 27017.

## Launch
Clone this repository:

```bash
git clone https://github.com/marshall245/fdic_data.git
```

Enter into the repo with:
```bash
cd fdic_data
```

Once inside the repo run (5 - 10 minutes run time):
```bash
docker-compose build
```

Once the containers are built run:
```bash
docker-compose up
```

Wait to see the following line at the console log:
```
jupyter-app_1  | [I 14:01:18.142 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
```

With the containers built access is available through any web browser at url:

```http://localhost:5000```

Login with the password:

```training```

## Project Structure and Use
Once inside the notebook you can click on 5 different files. One of those files is a set of helpers and not necessary for review. The other 4 are meant to be viewed in the following order:

* ffeic_data_first_look.ipynb
* review_asset_growth.ipynb
* easy_tables_for_analysts.ipynb
* querying_in_the_cosmosdb_sql_syntax.ipynb

The notebooks are meant to demonstrate the document structure and how the the tables may be accessed with increasing ease through different techniques.

NOTE: To run an individual cell, the user may simply select the cell and press ```<SHIFT> + <ENTER>``` . It may be easier, however, to just go to the kernel drop down at the top of a notebook and select "Restart and Run All"

I hope you enjoy the project :-)