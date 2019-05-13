FROM rabbitmq:3.7-management

RUN apt-get update && \
apt-get install -y curl

WORKDIR /opt/rabbitmq/plugins/

RUN curl -LO "https://github.com/deadtrickster/prometheus_rabbitmq_exporter/releases/download/v3.7.2.4/accept-0.3.3.ez"
RUN curl -LO "https://github.com/deadtrickster/prometheus_rabbitmq_exporter/releases/download/v3.7.2.4/prometheus-3.5.1.ez"
RUN curl -LO "https://github.com/deadtrickster/prometheus_rabbitmq_exporter/releases/download/v3.7.2.4/prometheus_cowboy-0.1.4.ez"
RUN curl -LO "https://github.com/deadtrickster/prometheus_rabbitmq_exporter/releases/download/v3.7.2.4/prometheus_httpd-2.1.8.ez"
RUN curl -LO "https://github.com/deadtrickster/prometheus_rabbitmq_exporter/releases/download/v3.7.2.4/prometheus_rabbitmq_exporter-3.7.2.4.ez"

RUN rabbitmq-plugins enable prometheus_rabbitmq_exporter
