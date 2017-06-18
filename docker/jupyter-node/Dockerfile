# Use an official conda:ubuntu runtime as a base image
FROM continuumio/miniconda3

EXPOSE 5000

# Set entry point to /bin/bash
ENTRYPOINT ["/bin/bash"]

# Set working directory
RUN mkdir /home/working
WORKDIR /home/working

# generate environment and launch it
RUN conda create -y -n working python=3 pymongo pandas jupyter matplotlib

# set new entry point
ENTRYPOINT ["/bin/bash", "-c"]

# set the notebook configs
# notebook password is "training"
RUN /opt/conda/envs/working/bin/jupyter notebook --allow-root --generate-config

RUN sed -i "s/#c.NotebookApp.ip = 'localhost'/c.NotebookApp.ip = '0.0.0.0'/" /root/.jupyter/jupyter_notebook_config.py

RUN sed -i "s/#c.NotebookApp.open_browser = True/c.NotebookApp.open_browser = False/" /root/.jupyter/jupyter_notebook_config.py

RUN sed -i "s/#c.NotebookApp.password = ''/c.NotebookApp.password = 'sha1:5878bec01ff1:6f07b05411eaee6c51b58e003775d650bca57bd5'/" /root/.jupyter/jupyter_notebook_config.py

RUN sed -i "s/#c.NotebookApp.port = 8888/c.NotebookApp.port = 5000/" /root/.jupyter/jupyter_notebook_config.py

# bring in data and scripts for building the database collections
ADD /financial_data_raw /home/working/financial_data_raw

ADD /db_build /home/working/db_build

ADD /notebooks /home/working/notebooks

WORKDIR /home/working/notebooks

# transform the fdic data and launch the notebook
CMD ["source activate working && python ../db_build/populate_data.py && source activate working && jupyter notebook --allow-root"]
