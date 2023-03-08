package config

type messageQueueConfig struct {
  Kafka map[string]struct {
    BootstrapServers string `yaml:"bootstrap.servers"`
    GroupId string `yaml:"group.id"`
    AutoOffsetReset string `yaml:"auto.offset.reset"`
  } `yaml:"kafka"`
  RabbitMQ map[string]struct {
    Queue string `yaml:"queue"`
    URL string `yaml:"url"`
  } `yaml:"rabbitmq"`
}
