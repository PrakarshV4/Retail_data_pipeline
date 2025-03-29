FROM jupyter/pyspark-notebook
WORKDIR /home
COPY . .
RUN pip install pandas numpy matplotlib sqlalchemy faker psycopg2-binary seaborn
EXPOSE 8888

CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]