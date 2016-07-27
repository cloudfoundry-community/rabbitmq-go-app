# rabbitmq-go-app
This simple CF go app uses the amqp(https://github.com/streadway/amqp) library to send and receive message to a rabbitmq service on Cloud Foundry.

## Requirments
- Need to have rabbitmq service on CF space
- Update manifest.yml to have the right rabbitmq SERVICE_NAME

## Usage
```
go build
cf push
```
Visit the URL and if it says "OK" then your rabbitmq is good!
