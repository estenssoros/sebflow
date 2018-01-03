docker run -it \
  -e MYSQL_ROOT_PASSWORD=sebflow132435 \
  -e MYSQL_DATABASE=sebflow \
  -e MYSQL_USER=seb \
  -e MYSQL_PASSWORD=sebflow132435 \
  -e MYSQL_PORT=3306 \
  -p 3306:3306 \
  mysql:5.7
