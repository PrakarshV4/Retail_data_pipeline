import mysql.connector

connection = mysql.connector.connect(
    user= "retail_user",
    password='retailpass' ,
    host='mysql',
    port='3306',
    database='retail_dw'
)

cursor = connection.cursor()
cursor.execute('Select * from dim_category')
x = cursor.fetchall()
print(x)