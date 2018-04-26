# Ziggurat contributing 


## Dev Setup (For mac users only)

- Install leiningen
```$ brew install leiningen```

- Install & setup rabbitmq 
```
$ brew install rabbitmq
$ nohup rabbitmq-server > rabbitmq-server.log & disown
$ rabbitmq-plugins enable rabbitmq_management
$ rabbitmqctl add_user gojek password
$ rabbitmqctl set_user_tags gojek administrator
$ rabbitmqctl set_permissions -p / gojek ".*" ".*" ".*"

```

- Run tests

```$ lein test```

Write code and raise PR :)
