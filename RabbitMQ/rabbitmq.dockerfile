FROM rabbitmq:3.10.2-management

RUN apt-get update

RUN apt-get install -y curl

RUN curl -L https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/3.10.2/rabbitmq_delayed_message_exchange-3.10.2.ez > $RABBITMQ_HOME/plugins/rabbitmq_delayed_message_exchange-3.10.2.ez

RUN echo '[rabbitmq_delayed_message_exchange,rabbitmq_management,rabbitmq_prometheus].' > /etc/rabbitmq/enabled_plugins
