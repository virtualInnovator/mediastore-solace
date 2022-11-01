# mediastore-solace
An event driven web application to create media(i.e. image) with links(or store in disk or obeject store, i.e. in future). Solace pubsub+ is used as the event broker platform

Both mediastore-hub and mediastore-spoke can run independently however this demo application was done to solve a hub-spoke architecture use case with solace pubsub+ cloud. If you want to try out that architecture you need to run the apps (python app for hub + react app for hub and same goes for spoke) on two different servers(one is called hub and other is called spoke)

Below goes the architecture

![mediastore-activemq-mediastore-app](https://user-images.githubusercontent.com/1380957/199182706-2d95e4a3-47a1-4de2-bac9-4f6d5dac3dcc.jpg)



