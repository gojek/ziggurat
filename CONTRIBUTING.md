# Ziggurat contributing 


## Dev Setup (For mac users only)

- Install leiningen: ```brew install leiningen```

- Install & setup rabbitmq 
```bash
brew install rabbitmq
nohup rabbitmq-server > rabbitmq-server.log & disown
rabbitmq-plugins enable rabbitmq_management
rabbitmqctl add_user gojek password
rabbitmqctl set_user_tags gojek administrator
rabbitmqctl set_permissions -p / gojek ".*" ".*" ".*"

```

- Run tests: ```lein test```

Write code and raise PR :)

Please add all your configs into ziggurat namespace in config.edn(test) file

If you PR is accepted then whatever configs you have added,
Please add them to deafult actor configs or ask Lambda team to do so and In actor template also
