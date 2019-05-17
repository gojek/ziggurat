## Upgrade Guide to Ziggurat 3.x from lower version

There were some breaking changes to kafka streams library being used by Ziggurat version 3.x. Now Ziggurat will uses kafka streams library version 2.x. This guide will show you how to upgrade your Ziggurat to version 3.x.  

For upgrading to Ziggurat version 3.x, all you need to do is to provide below configuration in your application

NOTE: You have to do it for all the topic entities

#### For version <= 2.7.2
You will need to add config `ZIGGURAT_STREAM_ROUTER_<TOPIC_ENTITY>_UPGRADE_FROM` to `0.11.0` 

#### For version > 2.7.2
You will need to add config `ZIGGURAT_STREAM_ROUTER_<TOPIC_ENTITY>_UPGRADE_FROM` to `1.1`

Once all of required configurations are supplied, you just need to deploy it. 
You will need to do rolling deployment in all of your instances. As reference you can see from this [doc](https://kafka.apache.org/20/documentation/streams/upgrade-guide). After every instances of your application are deployed successfully then you will have to remove the config and then redeploy your instances. 
