# polling_evaluation

## Environment Setup

Setup requires privileges to run docker.

```shell
$ docker pull jupyter/pyspark-notebook:42f4c82a07ff
$ docker run -d -v ${PWD}:/home/jovyan/work --network=host jupyter/pyspark-notebook:42f4c82a07ff
$ docker logs <container id>
```

Go to URL from log output and run `polling_evaluation.ipynb`.
