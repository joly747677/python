##删除多余镜像
docker rm -v $(docker ps --all --quiet --filter  ancestor=python_joly:1.10);