docker build . -f Dockerfile.hdfs -t p4-hdfs
docker build . -f Dockerfile.namenode -t p4-nn
docker build . -f Dockerfile.datanode -t p4-dn
docker build . -f Dockerfile.mysql -t p4-mysql
docker build . -f Dockerfile.server -t p4-server
docker compose up -d
