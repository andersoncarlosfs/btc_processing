# Introduction
Le bitcoin (BTC) est une monnaie virtuelle dont il existe des cours d'échange vers les monnaies fiduciaires, comme l'euro ou le dollar. C'est une monnaie complètement transparente, ce qui signifie qu'il est possible de suivre en direct toutes les transactions réalisées en bitcoin.

Mais d'où les bitcoins viennent-ils ? Pour les générer (on dit : "miner") il faut réaliser des calculs complexes qui nécessitent d’importantes ressources en CPU et en GPU, et donc du temps. Comme pour les transactions, le minage des bitcoins est public : lorsqu'un bloc de bitcoins est découvert tout le monde est mis au courant en même temps et on peut bien souvent savoir quelle est l'identité du mineur.

# Resources
- Docker
- Kafka 
- Storm

# Build
The containers will automatically built in the first run.

## Deployment
```
$docker-compose up
```

### Detached or foreground running 
```
$docker-compose up -d
```
# Results
The results are outputted by API call or written in a volume. An improvement could be the broadcasting of the results.

## API call
The results can be visualised from "[http://0.0.0.0:5001/view]('http://0.0.0.0:5001/view')".

## Volume
The results are written in a volume ending with *"_streamflow"*. Before accesing the files in the folder "output", the volume ending with *"_streamflow"* should be attached to a container, for instance: "busybox".

### Mounting 
```
$docker run -it -v Lalamove_streamflow:/volume ubuntu bin/bash
```

### Reading
```
$cd volume/output/
```

# Failover
- If one of the services crashes, it will be restarted automatically by docker-compose (Daemon)

# Scaling
- Deployment of more nodes in a cluster Spark cluster (workers) and/or Kafka cluster (brokers). 
- Broadcasting the results via Kafka

# Limitations
- The consumer gets the data at once. However, it supports stream processing

# Copyrights
- OpenClassrooms


