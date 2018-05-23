# Deploying M3DB

## Deploying a single node

Deploying a single-node cluster is a great way to experiment with M3DB and get a feel for what it
has to offer. Our Docker image by default configures a single M3DB instance with an embedded etcd
server (used for runtime configuration flags and topology management). This will allow you to read
and write timeseries data and change the topology of the instance.

To begin, first start up a Docker container with port `7201` (used to manage the cluster topology)
and port `9003` (used to read and write metrics) exposed. We recommend you create a persistent data
directory on your host for durability:

```
docker run -p 7201:7201 -p 9003:9003 --name m3db -v $(pwd)/m3db_data:/var/lib/m3db quay.io/m3db/m3db:latest
```

<!-- TODO: link to docs containing explanations of what namespaces, the coordinator,
placements, etc. are -->

<!-- TODO: add something about how this is in no way a recommended production deployment guide,
and write a guide for what is considered a production-ready deployment (this is in the works) -->

Next, create an initial namespace for your metrics:

```
curl -X POST localhost:7201/namespace/add -d '{
  "name": "default",
  "options": {
    "bootstrapEnabled": true,
    "flushEnabled": true,
    "writesToCommitLog": true,
    "enabledEnabled": true,
    "retentionOptions": {
      "retentionPeriodNanos": 172800000000000,
      "blockSizeNanos": 7200000000000,
      "bufferFutureNanos": 600000000000,
      "bufferPastNanos": 600000000000,
      "blockDataExpiry": true,
      "blockDataExpiryAfterNotAccessPeriodNanos": 300000000000
    },
    "snapshotEnabled": false,
    "indexOptions": {
      "enabled": true,
      "blockSizeNanos": 7200000000000
    }
  }
}'
```

With a namespace to hold your metrics created, you can initialize your first placement:

```
curl -X POST localhost:7201/placement/init -d '{
    "num_shards": 64,
    "replication_factor": 1,
    "instances": [
        {
            "id": "m3db_local",
            "isolation_group": "rack-a",
            "zone": "embedded",
            "weight": 1024,
            "endpoint": "127.0.0.1:9000",
            "hostname": "127.0.0.1",
            "port": 9000
        }
    ]
}'
```

Shortly after, you should see your node complete bootstrapping! Don't worry if you see warnings or
errors related to a local cache file. Those are expected for a local instance.

```
20:10:12.911218[I] updating database namespaces [{adds [default]} {updates []} {removals []}]
20:10:13.462798[I] node tchannelthrift: listening on 0.0.0.0:9000
20:10:13.463107[I] cluster tchannelthrift: listening on 0.0.0.0:9001
20:10:13.747173[I] node httpjson: listening on 0.0.0.0:9002
20:10:13.747506[I] cluster httpjson: listening on 0.0.0.0:9003
20:10:13.747763[I] bootstrapping shards for range starting ...
...
20:10:13.757834[I] bootstrap finished [{namespace default} {duration 10.1261ms}]
20:10:13.758001[I] bootstrapped
20:10:14.764771[I] successfully updated topology to 1 hosts
```

Now you can experiment with writing tagged metrics:
```
curl http://localhost:9003/writetagged -s -X POST -d '{"namespace":"default","id":"foo","tags":[{"name":"city","value":"new_york"},{"name":"endpoint","value":"/request"}],"datapoint":{"timestamp":'"$(date +"%s")"',"value":42.123456789}}'
```

And reading the metrics you've written:
```
$ curl http://localhost:9003/query -s -X POST -d '{"namespace":"default","query":{"regexp":{"field":"city","regexp":".*"}},"rangeStart":0,"rangeEnd":'"$(date +"%s")"'}' | jq .
{
  "results": [
    {
      "id": "foo",
      "tags": [
        {
          "name": "city",
          "value": "new_york"
        },
        {
          "name": "endpoint",
          "value": "/request"
        }
      ],
      "datapoints": [
        {
          "timestamp": 1527039389,
          "value": 42.123456789
        }
      ]
    }
  ],
  "exhaustive": true
}
```
