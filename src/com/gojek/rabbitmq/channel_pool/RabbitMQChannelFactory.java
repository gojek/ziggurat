package gojek.rabbitmq.channel_pool;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.DestroyMode;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMQChannelFactory extends BasePooledObjectFactory<Channel> {
    Logger log = LoggerFactory.getLogger(this.getClass());

    private final Connection connection;

    public RabbitMQChannelFactory(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Channel create() throws Exception {
        log.info("Creating a new channel");
        return connection.createChannel();
    }

    @Override
    public PooledObject<Channel> wrap(Channel channel) {
        return new DefaultPooledObject<Channel>(channel);
    }

    @Override
    public void destroyObject(PooledObject<Channel> p, DestroyMode destroyMode) throws Exception {
        super.destroyObject(p, destroyMode);
        log.info("Closing the channel with id: " + p.getObject().getChannelNumber());
        p.getObject().close();
    }
}
