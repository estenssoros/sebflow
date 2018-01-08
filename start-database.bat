docker run -d^
  -e POSTGRES_ROOT_PASSWORD=sebflow132435^
  -e POSTGRES_DB=sebflow^
  -e POSTGRES_USER=seb^
  -e POSTGRES_PASSWORD=sebflow132435^
  -v structure.sql:/docker-entrypoint-initdb.d^
  -p 5431:5432^
  postgres:10.1
